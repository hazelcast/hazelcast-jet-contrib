SELECT dss.[status], dss.[status_desc]
FROM   sys.dm_server_services dss
WHERE  dss.[servicename] LIKE N'SQL Server Agent (%';
GO

SELECT name, database_id, is_cdc_enabled FROM sys.databases
WHERE is_cdc_enabled = 1 AND name = 'MyDB'
GO

