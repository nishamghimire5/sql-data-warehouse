/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
    - Converts the data types of columns to match the target table schema. (in sales table-dateFormat)
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    begin try
    set @batch_start_time = getdate();
    print('==================================================');
    PRINT('Loading data into bronze layer...');
    print('==================================================');

    PRINT('--------------------------------------------------');
    PRINT('Loading CRM Tables...');
    PRINT('--------------------------------------------------');
    
    
    SET @start_time = GETDATE();
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT('Loading Data into Table: bronze.crm_cust_info');
    bulk insert bronze.crm_cust_info
from 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    tablock
);
    SET @end_time = GETDATE();
    PRINT('Time taken to load bronze.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
PRINT('----------------------------------------------');

set @start_time = getdate();
    PRINT('Truncating Table: bronze.crm_prd_info');
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT('Loading Data into Table: bronze.crm_prd_info');
    bulk insert bronze.crm_prd_info
from 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    tablock
);
set @end_time = getdate();
    PRINT('Time taken to load bronze.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
    PRINT('----------------------------------------------');
    SET @start_time = GETDATE();
    PRINT('Truncating Table: bronze.crm_sales_details');
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT('Creating Table: bronze.crm_sales_details_staging');
    IF OBJECT_ID('bronze.crm_sales_details_staging', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details_staging;
    CREATE TABLE bronze.crm_sales_details_staging
    (
        sls_ord_num NVARCHAR(50),
        sls_prd_key NVARCHAR(50),
        sls_cust_id INT,
        sls_order_dt NVARCHAR(50),
        sls_ship_dt NVARCHAR(50),
        sls_due_dt NVARCHAR(50),
        sls_sales INT,
        sls_quantity INT,
        sls_price INT
    );

    PRINT('Loading Data into Table: bronze.crm_sales_details_staging');
    BULK INSERT bronze.crm_sales_details_staging
FROM 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

    PRINT('Inserting Data into Table: bronze.crm_sales_details');
    INSERT INTO bronze.crm_sales_details
        (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        TRY_CONVERT(DATE, sls_order_dt, 112) AS sls_order_dt, -- YYYYMMDD format
        TRY_CONVERT(DATE, sls_ship_dt, 112) AS sls_ship_dt,
        TRY_CONVERT(DATE, sls_due_dt, 112) AS sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    FROM
        bronze.crm_sales_details_staging;

    PRINT('Dropping Table: bronze.crm_sales_details_staging');
    DROP TABLE bronze.crm_sales_details_staging;
set @end_time = getdate();
    PRINT('Time taken to load bronze.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');

    PRINT('--------------------------------------------------');
    PRINT('Loading ERP Tables...');
    PRINT('--------------------------------------------------');

set @start_time = getdate();
    PRINT('>> Truncating Table: bronze.erp_loc_a101');
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT('>> Loading Data into Table: bronze.erp_loc_a101');
    bulk insert bronze.erp_loc_a101
from 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    tablock
);
set @end_time = getdate();
    PRINT('Time taken to load bronze.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
    PRINT('----------------------------------------------');
    
    set @start_time = getdate();
    print('>> Truncating Table: bronze.erp_cust_az12');
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT('>> Loading Data into Table: bronze.erp_cust_az12');
    bulk insert bronze.erp_cust_az12
from 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    tablock
);
set @end_time = getdate();
    PRINT('Time taken to load bronze.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
    PRINT('----------------------------------------------');

    set @start_time = getdate();

    PRINT('>> Truncating Table: bronze.erp_px_cat_g1v2');
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT('>> Loading Data into Table: bronze.erp_px_cat_g1v2');
    bulk insert bronze.erp_px_cat_g1v2
from 'C:\Users\ghimi\Downloads\Compressed\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (
    firstrow = 2,
    fieldterminator = ',',
    tablock
);
    set @end_time = getdate();
    PRINT('Time taken to load bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
    PRINT('----------------------------------------------');
    set @batch_end_time = getdate();

    PRINT('Time taken to load all tables: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');
    PRINT('==================================================');
    PRINT('Data loaded successfully into bronze layer...');
    PRINT('==================================================');
end try
begin catch
    PRINT('==================================================');
    print('An error occurred during loading bronze layer...');
    PRINT('Error Message: ' + CAST(ERROR_MESSAGE() AS NVARCHAR));
    PRINT('Error Message: ' + CAST(ERROR_LINE() AS NVARCHAR));
    PRINT('Error State: ' + CAST(ERROR_STATE() AS NVARCHAR));
    PRINT('An error occurred: ' + ERROR_MESSAGE());
    PRINT('==================================================');
end catch
END
