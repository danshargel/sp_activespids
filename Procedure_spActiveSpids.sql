USE master;
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO


CREATE OR ALTER PROCEDURE dbo.sp_Activespids
    @IncludeBlocking BIT = NULL,
    @ShowPlan BIT = 1,
    @Fast BIT = 0
AS
EXEC DBAdmin.dbo.Activespids @SortCriteria = NULL,
                             @IncludeBlocking = @IncludeBlocking,
                             @ShowPlan = @ShowPlan,
                             @Fast = @Fast;
GO

EXEC sys.sp_MS_marksystemobject sp_Activespids;
GO
