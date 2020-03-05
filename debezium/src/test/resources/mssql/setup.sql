CREATE DATABASE MyDB;
GO


USE MyDB
GO
EXEC sys.sp_cdc_enable_db
GO

USE MyDB
GO
CREATE SCHEMA inventory;
GO
CREATE TABLE inventory.customers(
   id INTEGER IDENTITY(1001,1) NOT NULL PRIMARY KEY,
   first_name VARCHAR(255) NOT NULL,
   last_name VARCHAR(255) NOT NULL,
   email VARCHAR(255) NOT NULL UNIQUE
);
GO


EXEC sys.sp_cdc_enable_table
@source_schema = N'inventory',
@source_name   = N'customers',
@role_name     = NULL,
@supports_net_changes = 0
GO

EXEC sys.sp_cdc_help_change_data_capture
GO

USE MyDB
INSERT INTO inventory.customers  (first_name, last_name, email) VALUES ('Anne', 'Kretchmar', 'annek@noanswer.org');
GO


