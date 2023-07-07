USE [DBAdmin]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO


CREATE OR ALTER procedure [dbo].[ActiveSpids]  @SortCriteria VarChar(50) = Null, @IncludeBlocking Bit = 1, @ShowPlan bit = 0, @Fast bit = 0
as
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
**************************************************************************************************************************************************************************************/

---------------------------------------------
-- declare variables
---------------------------------------------
declare @SQL varchar(4000), @ErrMsg VarChar(150)

---------------------------------------------
-- set session variables
---------------------------------------------
set nocount on

---------------------------------------------
-- body of stored procedure
---------------------------------------------
If @SortCriteria Is Null
	Set @SortCriteria = 'start_time'

If @IncludeBlocking Is Null
	Set @IncludeBlocking = 1


--Ensure appropriate parameters are being passed:
If @SortCriteria Not In ('SPID', 'session_id', 'start_time', 'BatchStartTime','TranStartTime')
Begin
Set @ErrMsg = 'Only the following are acceptable for @SortCriteria: ' + Char(13) + Char(10)
Set @ErrMsg = @ErrMsg + 'SPID, session_id, start_time, BatchStartTime'
GoTo QuitWithError
End

-----------------------------------------------------------------------------------------------------------
-- If blocking from schema lock or more than 5 spids blocked, use lighter-weight query to avoid being 
-- blocked.  Occasionally this query will hang when blocking occurs, haven't been able to reproduce it
-- so attempting to work around it.
-----------------------------------------------------------------------------------------------------------
if exists(
	Select  1 from sys.dm_exec_requests where blocking_session_id <> 0 and wait_type = 'LCK_M_SCH_S')
	or (select count(*)  from sys.dm_exec_requests where blocking_session_id <> 0) > 5
set @Fast = 1


if @Fast = 0
begin
Select * From
	(Select
		R.session_id as [SPID]
		, R.start_time as BatchStartTime
		, datediff(ss, R.start_time, getDate())/60 as BatchMin
		, T.transaction_begin_time as TranStartTime
		, datediff(ss, T.transaction_begin_time, getDate())/60 as TranMin
		, S.login_name as [Login]
		, S.host_name as [Host]
		, R.Blocking_session_id as blocker
		, db_name(r.database_id) as dbName
		, object_schema_name (qt.objectid,qt.[dbid]) as schm
		, object_name(qt.objectid, qt.[dbid]) as procName
		, ib.event_info  as InputBuffer
		,(select top 1 SUBSTRING(qt.[text],statement_start_offset / 2+1 , 
		  ( (case when statement_end_offset <= 0 
			 then (LEN(CONVERT(nvarchar(max),qt.[text])) * 2) 
			 else statement_end_offset end)  - statement_start_offset) / 2+1)) As statement_text
		, R.wait_type
		, R.status
		, R.command
		, R.cpu_time
		, R.reads
		, R.writes
		, R.start_time
		, S.last_request_start_time
		, m.scheduler_id
		, m.requested_memory_kb
		, m.granted_memory_kb
		, m.query_cost
		,convert(varchar(50),'('+convert(varchar(5),wait_time)+'ms)')+wait_resource as wait_resource
		, (R.estimated_completion_time/1000)/60.0 as ect_min
	    , R.percent_complete
		, R.open_transaction_count
		, u.user_objects_alloc_page_count
		, u.user_objects_dealloc_page_count
		, u.internal_objects_alloc_page_count
		, u.internal_objects_dealloc_page_count
		, R.total_elapsed_time
		, Case
			When @SortCriteria In ('SPID', 'session_id') Then Cast(R.session_id As VarBinary(50))
			When @SortCriteria In ('BatchStartTime', 'start_time') Then Cast(R.start_time As VarBinary(50))
			When @SortCriteria In ('TranStartTime', 'Tran') Then Cast(IsNull(datediff(mi, T.transaction_begin_time, getDate()),0) As VarBinary(50))
		End As SortCriteria
		, Case 
			when @ShowPlan = 1 then qp.Query_plan
			when @ShowPlan = 0 then 'Query Plan Not Requested: @ShowPlan = 0'
			end as QueryPlan
		--, qp.Query_plan
	from sys.dm_exec_requests R 
	left outer join sys.dm_exec_sessions S
		on R.Session_id = S.Session_id
	left outer join sys.dm_tran_active_transactions t
	on r.transaction_id = t.transaction_id
	left outer join sys.dm_exec_query_memory_grants m
	on S.Session_id = m.session_id
	and R.request_id = m.request_id
	left outer join sys.dm_db_session_space_usage u
	on S.Session_id = u.session_id

		Cross Apply sys.dm_exec_sql_text(R.sql_handle) As qt
		Outer Apply sys.dm_exec_query_plan(R.plan_handle) AS qp
		Cross Apply sys.dm_exec_input_buffer(S.session_id, NULL) AS ib  

	where (S.is_user_process = 1
	and R.Session_id <> @@spid
	and r.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
	)
	or R.open_transaction_count > 0 
	 ) As Active

	Order By SortCriteria Asc;
end

if @Fast = 1
begin
Select * From
	(Select
		R.session_id as [SPID]
		, R.start_time as BatchStartTime
		, datediff(ss, R.start_time, getDate())/60 as BatchMin
		, T.transaction_begin_time as TranStartTime
		, datediff(ss, T.transaction_begin_time, getDate())/60 as TranMin
		, S.login_name as [Login]
		, S.host_name as [Host]
		, R.Blocking_session_id as blocker
		, db_name(r.database_id) as dbName
		, ib.event_info  as InputBuffer
		, R.wait_type
		, R.status
		, R.command
		, R.cpu_time
		, R.reads
		, R.writes
		, R.start_time
		, S.last_request_start_time
		, m.scheduler_id
		, m.requested_memory_kb
		, m.granted_memory_kb
		, m.query_cost
		,convert(varchar(50),'('+convert(varchar(5),wait_time)+'ms)')+wait_resource as wait_resource
		, (R.estimated_completion_time/1000)/60.0 as ect_min
	    , R.percent_complete
		, R.open_transaction_count
		, R.total_elapsed_time
		, Case
			When @SortCriteria In ('SPID', 'session_id') Then Cast(R.session_id As VarBinary(50))
			When @SortCriteria In ('BatchStartTime', 'start_time') Then Cast(R.start_time As VarBinary(50))
			When @SortCriteria In ('TranStartTime', 'Tran') Then Cast(IsNull(datediff(mi, T.transaction_begin_time, getDate()),0) As VarBinary(50))
		End As SortCriteria
	
	from sys.dm_exec_requests R 
	left outer join sys.dm_exec_sessions S
		on R.Session_id = S.Session_id
	left outer join sys.dm_tran_active_transactions t
	on r.transaction_id = t.transaction_id
	left outer join sys.dm_exec_query_memory_grants m
	on S.Session_id = m.session_id
	and R.request_id = m.request_id

	Cross Apply sys.dm_exec_input_buffer(S.session_id, NULL) AS ib  

	where (S.is_user_process = 1
	and R.Session_id <> @@spid
	and r.wait_type <> 'SP_SERVER_DIAGNOSTICS_SLEEP'
	)
	or R.open_transaction_count > 0 
	 ) As Active
	
	Order By SortCriteria Asc;

end



If @IncludeBlocking = 1
Begin
	With Blocking (SPID, Blocker) As
	(
	Select Distinct R2.Blocking_session_id As SPID, 0 As Blocker
	From sys.dm_exec_requests R1
	Join sys.dm_exec_requests R2 On R1.session_id = R2.Blocking_session_id
	And R1.Blocking_session_id = 0
	Union
	Select session_id As SPID, Blocking_session_id As Blocker
	From sys.dm_exec_requests R 
	Where Blocking_session_id <> 0
	)
	Select * From Blocking Order By Blocker Asc
End

Return

QuitWithError:
Declare @MsgTxt VarChar(150)
Select @MsgTxt = IsNull(@ErrMsg, 'Unspecified Error')
RaisError(@MsgTxt, 15, 1)
Return






GO


