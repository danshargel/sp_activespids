SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER OFF;
GO



CREATE OR ALTER PROCEDURE dbo.ActiveSpids
    @SortCriteria VARCHAR(50) = NULL,
    @IncludeBlocking BIT = 1,
    @ShowPlan BIT = 0,
    @Fast BIT = 0,
    @IncludeSleepers BIT = 1
AS
/**************************************************************************************************************************************************************************************
*   DBAdmin.dbo.ActiveSpids 
*   Creator:       DBA Team
*   Date:          2/8/2007
*   Project:       Database Monitoring
*   Notes:         A lighter-weight version of ActiveSpids using Dynamic Management Views
*					**Any sort criteria additions need to be added to the validation statement, error text, and case statement
*
*   Usage:
*       execute DBAdmin.dbo.ActiveSpids @SortCriteria = 'SPID', @IncludeBlocking = 0
*
*	Modifications:   
*   Developer Name     Date     Brief Description
*   ------------------ -------- ------------------------------------------------------------
*   Oliver Chappell	2/21/2008	Changed so it didn't used DSQL
*   Jeremy Broestl	8/12/2008	Added wait_type
*	David Zahner	7/29/2009	Changed the blocking list to display lead blockers first, also removing unnecessary joins and @@spid exclusion
*	David Zahner	10/27/2009	Added parameters for sorting and including blocking list, changed it to use DBAdmin.dbo.fn_StatementFromHandle
*								to eliminate the substring errors, added validations for parameters and function dependancy with GoTo, 
*								changed version check to look for anything greater than 2000 instead of just 2005 (was throwing error on 2008) 
*   Dan Shargel      2/22/2012  Added transaction_begin_time from sys.dm_tran_active_transactions to better determine
*                                 batched vs long-running unbatched processes
*   Dan Shargel      3/14/2012  Added columns from dm_exec_query_memory_grants and dm_db_session_space_usage
*   Dan Shargel      4/20/2012  Added estimated_completion_time (as ect_min) and percent_complete
*   Dan Shargel     12/21/2012  Updated BatchMin and TranMin to get seconds/60 for the minute 
*   Dan Shargel     02/28/2013  Added object_schema_name to output
*	David Zahner	03/21/2013	Removed fn_StatementFromHandle function dependancy and changed statement_text back to substring with case
*   Dan Shargel     01/05/2015  Filter out waitfor wait_type
*   Dan Shargel     04/08/2016  Changed database name to come from database id of sys.dm_exec_requests
*                                 instead of sys.dm_exec_sql_text as it was blank for some sessions.
*                                  Added @ShowPlan option to decided whether or not to display the query plan.
*   Dan Shargel     05/06/2016  Removed filter on r.wait_type <> 'WAITFOR'. Was removing processes with NULL wait_type.
*   Dan Shargel     11/07/2016  Added @Fast parameter to run lighter-weight query of DMV's so that can perform less work if server busy
*                                 or there are blocking chains involving schema locks which prevent this from completing.
*   Dan Shargel     11/08/2016  Make it obvious when query plan is not requested with: "when @ShowPlan = 0 then 'Query Plan Not Requested: @ShowPlan = 0'"
*   Dan Shargel     01/26/2017  Make use of new sys.dm_exec_input_buffer function to return the "input buffer".
*                                Changed join from cross to outer apply to sys.dm_exec_query_plan to not filter out "alter index". Was leaving out queries
*                                that did not have a query plan.
*  Dan Shargel      01/23/2018  Filter out SP_SERVER_DIAGNOSTICS_SLEEP 
*  Dan Shargel      07/19/2023  Add option to include idle sessions with open transactions @IncludeSleepers
**************************************************************************************************************************************************************************************/

---------------------------------------------
-- declare variables
---------------------------------------------
DECLARE @SQL VARCHAR(4000),
        @ErrMsg VARCHAR(150);

---------------------------------------------
-- set session variables
---------------------------------------------
SET NOCOUNT ON;

---------------------------------------------
-- body of stored procedure
---------------------------------------------
IF @SortCriteria IS NULL
    SET @SortCriteria = 'start_time';

IF @IncludeBlocking IS NULL
    SET @IncludeBlocking = 1;


--Ensure appropriate parameters are being passed:
IF @SortCriteria NOT IN ( 'SPID', 'session_id', 'start_time', 'BatchStartTime', 'TranStartTime' )
BEGIN
    SET @ErrMsg = 'Only the following are acceptable for @SortCriteria: ' + CHAR(13) + CHAR(10);
    SET @ErrMsg = @ErrMsg + 'SPID, session_id, start_time, BatchStartTime';
    GOTO QuitWithError;
END;

-----------------------------------------------------------------------------------------------------------
-- If blocking from schema lock or more than 5 spids blocked, use lighter-weight query to avoid being 
-- blocked.  Occasionally this query will hang when blocking occurs, haven't been able to reproduce it
-- so attempting to work around it.
-----------------------------------------------------------------------------------------------------------
IF EXISTS
(
    SELECT 1
    FROM sys.dm_exec_requests
    WHERE blocking_session_id <> 0
          AND wait_type = 'LCK_M_SCH_S'
)
   OR
   (
       SELECT COUNT(*)FROM sys.dm_exec_requests WHERE blocking_session_id <> 0
   ) > 5
    SET @Fast = 1;


IF @Fast = 0
BEGIN
    SELECT *
    FROM
    (
        SELECT R.session_id AS SPID,
               R.start_time AS BatchStartTime,
               DATEDIFF(ss, R.start_time, GETDATE()) / 60 AS BatchMin,
               t.transaction_begin_time AS TranStartTime,
               DATEDIFF(ss, t.transaction_begin_time, GETDATE()) / 60 AS TranMin,
               S.login_name AS Login,
               S.host_name AS Host,
               R.Blocking_session_id AS blocker,
               DB_NAME(R.database_id) AS dbName,
               OBJECT_SCHEMA_NAME(qt.objectid, qt.dbid) AS schm,
               OBJECT_NAME(qt.objectid, qt.dbid) AS procName,
               ib.event_info AS InputBuffer,
               (
                   SELECT TOP 1
                          SUBSTRING(   qt.text,
                                       statement_start_offset / 2 + 1,
                                       ((CASE
                                             WHEN statement_end_offset <= 0 THEN
                                       (LEN(CONVERT(NVARCHAR(MAX), qt.text)) * 2)
                                             ELSE
                                                 statement_end_offset
                                         END
                                        ) - statement_start_offset
                                       ) / 2 + 1
                                   )
               ) AS statement_text,
               R.wait_type,
               R.status,
               R.command,
               R.cpu_time,
               R.reads,
               R.writes,
               R.start_time,
               S.last_request_start_time,
               m.scheduler_id,
               m.requested_memory_kb,
               m.granted_memory_kb,
               m.query_cost,
               CONVERT(VARCHAR(50), '(' + CONVERT(VARCHAR(5), wait_time) + 'ms)') + wait_resource AS wait_resource,
               (R.estimated_completion_time / 1000) / 60.0 AS ect_min,
               R.percent_complete,
               R.open_transaction_count,
               u.user_objects_alloc_page_count,
               u.user_objects_dealloc_page_count,
               u.internal_objects_alloc_page_count,
               u.internal_objects_dealloc_page_count,
               R.total_elapsed_time,
               CASE
                   WHEN @SortCriteria IN ( 'SPID', 'session_id' ) THEN
                       CAST(R.session_id AS VARBINARY(50))
                   WHEN @SortCriteria IN ( 'BatchStartTime', 'start_time' ) THEN
                       CAST(R.start_time AS VARBINARY(50))
                   WHEN @SortCriteria IN ( 'TranStartTime', 'Tran' ) THEN
                       CAST(ISNULL(DATEDIFF(mi, t.transaction_begin_time, GETDATE()), 0) AS VARBINARY(50))
               END AS SortCriteria,
               CASE
                   WHEN @ShowPlan = 1 THEN
                       qp.Query_plan
                   WHEN @ShowPlan = 0 THEN
                       'Query Plan Not Requested: @ShowPlan = 0'
               END AS QueryPlan
        --, qp.Query_plan
        FROM sys.dm_exec_requests R
            LEFT OUTER JOIN sys.dm_exec_sessions S
                ON R.Session_id = S.Session_id
            LEFT OUTER JOIN sys.dm_tran_active_transactions t
                ON R.transaction_id = t.transaction_id
            LEFT OUTER JOIN sys.dm_exec_query_memory_grants m
                ON S.Session_id = m.session_id
                   AND R.request_id = m.request_id
            LEFT OUTER JOIN sys.dm_db_session_space_usage u
                ON S.Session_id = u.session_id
            CROSS APPLY sys.dm_exec_sql_text(R.sql_handle) AS qt
            OUTER APPLY sys.dm_exec_query_plan(R.plan_handle) AS qp
            CROSS APPLY sys.dm_exec_input_buffer(S.session_id, NULL) AS ib
        WHERE (
                  S.is_user_process = 1
                  AND R.Session_id <> @@spid
                  AND R.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
              )
              OR R.open_transaction_count > 0
    ) AS Active
    ORDER BY SortCriteria ASC;
END;

IF @Fast = 1
BEGIN
    SELECT *
    FROM
    (
        SELECT R.session_id AS SPID,
               R.start_time AS BatchStartTime,
               DATEDIFF(ss, R.start_time, GETDATE()) / 60 AS BatchMin,
               t.transaction_begin_time AS TranStartTime,
               DATEDIFF(ss, t.transaction_begin_time, GETDATE()) / 60 AS TranMin,
               S.login_name AS Login,
               S.host_name AS Host,
               R.Blocking_session_id AS blocker,
               DB_NAME(R.database_id) AS dbName,
               ib.event_info AS InputBuffer,
               R.wait_type,
               R.status,
               R.command,
               R.cpu_time,
               R.reads,
               R.writes,
               R.start_time,
               S.last_request_start_time,
               m.scheduler_id,
               m.requested_memory_kb,
               m.granted_memory_kb,
               m.query_cost,
               CONVERT(VARCHAR(50), '(' + CONVERT(VARCHAR(5), wait_time) + 'ms)') + wait_resource AS wait_resource,
               (R.estimated_completion_time / 1000) / 60.0 AS ect_min,
               R.percent_complete,
               R.open_transaction_count,
               R.total_elapsed_time,
               CASE
                   WHEN @SortCriteria IN ( 'SPID', 'session_id' ) THEN
                       CAST(R.session_id AS VARBINARY(50))
                   WHEN @SortCriteria IN ( 'BatchStartTime', 'start_time' ) THEN
                       CAST(R.start_time AS VARBINARY(50))
                   WHEN @SortCriteria IN ( 'TranStartTime', 'Tran' ) THEN
                       CAST(ISNULL(DATEDIFF(mi, t.transaction_begin_time, GETDATE()), 0) AS VARBINARY(50))
               END AS SortCriteria
        FROM sys.dm_exec_requests R
            LEFT OUTER JOIN sys.dm_exec_sessions S
                ON R.Session_id = S.Session_id
            LEFT OUTER JOIN sys.dm_tran_active_transactions t
                ON R.transaction_id = t.transaction_id
            LEFT OUTER JOIN sys.dm_exec_query_memory_grants m
                ON S.Session_id = m.session_id
                   AND R.request_id = m.request_id
            CROSS APPLY sys.dm_exec_input_buffer(S.session_id, NULL) AS ib
        WHERE (
                  S.is_user_process = 1
                  AND R.Session_id <> @@spid
                  AND R.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
              )
              OR R.open_transaction_count > 0
    ) AS Active
    ORDER BY SortCriteria ASC;

END;



IF @IncludeBlocking = 1
BEGIN
    WITH Blocking (SPID, Blocker)
    AS (SELECT DISTINCT
               R2.Blocking_session_id AS SPID,
               0 AS Blocker
        FROM sys.dm_exec_requests R1
            JOIN sys.dm_exec_requests R2
                ON R1.session_id = R2.Blocking_session_id
                   AND R1.Blocking_session_id = 0
        UNION
        SELECT session_id AS SPID,
               Blocking_session_id AS Blocker
        FROM sys.dm_exec_requests R
        WHERE Blocking_session_id <> 0)
    SELECT *
    FROM Blocking
    ORDER BY Blocker ASC;
END;


IF @IncludeSleepers = 1
BEGIN
    IF EXISTS
    (
        SELECT 1
        FROM sys.dm_tran_active_transactions at
            INNER JOIN sys.dm_tran_session_transactions st
                ON st.transaction_id = at.transaction_id
            LEFT OUTER JOIN sys.dm_exec_sessions sess
                ON st.session_id = sess.session_id
            LEFT OUTER JOIN sys.dm_exec_connections conn
                ON conn.session_id = sess.session_id
            OUTER APPLY sys.dm_exec_sql_text(conn.most_recent_sql_handle) AS txt
        WHERE DATEDIFF(SECOND, transaction_begin_time, GETDATE()) > 60
              AND is_user_process = 1
    )
        SELECT st.session_id,
               DATEDIFF(SECOND, transaction_begin_time, GETDATE()) AS tran_elapsed_time_seconds,
               txt.text,
               at.transaction_id,
               at.name,
               at.transaction_begin_time,
               at.transaction_type,
               at.transaction_status,
               sess.original_login_name,
               sess.host_name
        FROM sys.dm_tran_active_transactions at
            INNER JOIN sys.dm_tran_session_transactions st
                ON st.transaction_id = at.transaction_id
            LEFT OUTER JOIN sys.dm_exec_sessions sess
                ON st.session_id = sess.session_id
            LEFT OUTER JOIN sys.dm_exec_connections conn
                ON conn.session_id = sess.session_id
            OUTER APPLY sys.dm_exec_sql_text(conn.most_recent_sql_handle) AS txt
        WHERE DATEDIFF(SECOND, transaction_begin_time, GETDATE()) > 60
              AND is_user_process = 1;
END;



RETURN;

QuitWithError:
DECLARE @MsgTxt VARCHAR(150);
SELECT @MsgTxt = ISNULL(@ErrMsg, 'Unspecified Error');
RAISERROR(@MsgTxt, 15, 1);
RETURN;

