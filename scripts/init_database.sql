-- =========================================
-- Create DataWareHouse Database and Schemas
-- =========================================

-- Create or modify DataWareHouse DB
USE master;
IF EXISTS (SELECT *
FROM sys.databases
WHERE name = 'DataWareHouse')
    ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

-- Create schemas if they don't exist
IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze');
GO

IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');
GO

IF NOT EXISTS (SELECT *
FROM sys.schemas
WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO
