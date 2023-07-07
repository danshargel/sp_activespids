USE master;
GO

CREATE USER <LoginName, sysname, LoginName> FOR LOGIN <LoginName, sysname, LoginName>;
GO
GRANT CONNECT TO <LoginName, sysname, LoginName>;
GO
GRANT EXEC ON sp_activespids TO <LoginName, sysname, LoginName>;
GO
GRANT VIEW SERVER STATE TO <LoginName, sysname, LoginName>;
GO

USE DBAdmin;
GO

CREATE USER <LoginName, sysname, LoginName> FOR LOGIN <LoginName, sysname, LoginName>;
GO
GRANT CONNECT TO <LoginName, sysname, LoginName>;
GO
GRANT EXEC ON ActiveSpids TO <LoginName, sysname, LoginName>;
GO

