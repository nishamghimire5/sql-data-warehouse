/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from the 'bronze' layer. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Cleanses and transforms data from bronze tables.
    - Handles null values, standardizes formats, and applies business logic.
    - Inserts the transformed data into corresponding silver tables.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT('==================================================');
        PRINT('Loading data into silver layer...');
        PRINT('==================================================');

        PRINT('--------------------------------------------------');
        PRINT('Truncating Silver Tables...');
        PRINT('--------------------------------------------------');

        -- Truncate silver tables before loading
        PRINT('Truncating Table: silver.crm_cust_info');
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT('Truncating Table: silver.crm_prd_info');
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT('Truncating Table: silver.crm_sales_details');
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT('Truncating Table: silver.erp_cust_az12');
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT('Truncating Table: silver.erp_loc_a101');
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT('Truncating Table: silver.erp_px_cat_g1v2');
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT('--------------------------------------------------');
        PRINT('Transforming and Loading CRM Tables...');
        PRINT('--------------------------------------------------');

        -- Transform and Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.crm_cust_info');
        WITH
        LatestRecords
        AS
        (
            SELECT
                cst_id,
                cst_key,
                COALESCE(TRIM(cst_firstname), 'Unknown') AS cst_firstname,
                COALESCE(TRIM(cst_lastname), 'Unknown') AS cst_lastname,
                CASE 
                    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                END AS cst_marital_status,
                CASE 
                    WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
                    WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
                    ELSE 'n/a'
                END AS cst_gender,
                cst_create_date,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_Last
            FROM bronze.crm_cust_info
        )
    INSERT INTO silver.crm_cust_info
        (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gender,
        cst_create_date
    FROM LatestRecords
    WHERE flag_Last = 1 AND cst_id IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.crm_cust_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        -- Transform and Load crm_prd_info
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.crm_prd_info');
        WITH
        TransformedData
        AS
        (
            SELECT
                prd_id,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
                prd_nm,
                ISNULL(prd_cost, 0) AS prd_cost,
                CASE UPPER(TRIM(prd_line))
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
                END AS prd_line,
                CAST(prd_start_dt AS DATE) AS prd_start_dt,
                CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
            FROM bronze.crm_prd_info
        )
    INSERT INTO silver.crm_prd_info
        (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    FROM TransformedData;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.crm_prd_info: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        -- Transform and Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.crm_sales_details');
        INSERT INTO silver.crm_sales_details
        (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
                WHEN sls_order_dt > '2050-01-01' OR sls_order_dt < '1900-01-01' THEN DATEADD(DAY, -1, sls_ship_dt)
                ELSE sls_order_dt
            END AS sls_order_dt,
        CASE 
                WHEN sls_ship_dt > sls_due_dt THEN DATEADD(DAY, -1, sls_due_dt)
                ELSE sls_ship_dt
            END AS sls_ship_dt,
        sls_due_dt,
        CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 THEN 
                    CASE 
                        WHEN sls_quantity IS NOT NULL AND sls_quantity > 0 AND sls_price IS NOT NULL AND sls_price > 0 THEN sls_quantity * sls_price
                        ELSE NULL
                    END
                ELSE sls_sales
            END AS sls_sales,
        CASE 
                WHEN sls_quantity IS NULL OR sls_quantity <= 0 THEN 
                    CASE 
                        WHEN sls_sales IS NOT NULL AND sls_sales > 0 AND sls_price IS NOT NULL AND sls_price > 0 THEN sls_sales / sls_price
                        ELSE NULL
                    END
                ELSE sls_quantity
            END AS sls_quantity,
        CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN 
                    CASE 
                        WHEN sls_sales IS NOT NULL AND sls_sales > 0 AND sls_quantity IS NOT NULL AND sls_quantity > 0 THEN sls_sales / sls_quantity
                        ELSE NULL
                    END
                ELSE sls_price
            END AS sls_price
    FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.crm_sales_details: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        PRINT('--------------------------------------------------');
        PRINT('Transforming and Loading ERP Tables...');
        PRINT('--------------------------------------------------');

        -- Transform and Load erp_cust_az12
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.erp_cust_az12');
        INSERT INTO silver.erp_cust_az12
        (cid, bdate, gen)
    SELECT
        CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
                WHEN cid LIKE 'AWO%' THEN SUBSTRING(cid, 4, LEN(cid)) 
                ELSE cid 
            END AS cid,
        CASE 
                WHEN bdate > GETDATE() THEN NULL 
                ELSE bdate 
            END AS bdate,
        CASE 
                WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
    FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.erp_cust_az12: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        -- Transform and Load erp_loc_a101
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.erp_loc_a101');
        INSERT INTO silver.erp_loc_a101
        (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE 
                WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('USA', 'UNITED STATES', 'US') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = 'AUSTRALIA' THEN 'Australia'
                WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM', 'UK') THEN 'United Kingdom'
                WHEN UPPER(TRIM(cntry)) = 'CANADA' THEN 'Canada'
                WHEN UPPER(TRIM(cntry)) = 'FRANCE' THEN 'France'
                ELSE 'n/a'
            END AS cntry
    FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.erp_loc_a101: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        -- Transform and Load erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT('Transforming Data for Table: silver.erp_px_cat_g1v2');
        INSERT INTO silver.erp_px_cat_g1v2
        (id, cat, subcat, maintenace)
    SELECT
        id,
        cat,
        subcat,
        maintenace
    FROM bronze.erp_px_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT('Time taken to load silver.erp_px_cat_g1v2: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('----------------------------------------------');

        SET @batch_end_time = GETDATE();
        PRINT('Time taken to load all tables: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');
        PRINT('==================================================');
        PRINT('Data loaded successfully into silver layer...');
        PRINT('==================================================');
    END TRY
    BEGIN CATCH
        PRINT('==================================================');
        PRINT('An error occurred during loading silver layer...');
        PRINT('Error Message: ' + CAST(ERROR_MESSAGE() AS NVARCHAR));
        PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR));
        PRINT('Error State: ' + CAST(ERROR_STATE() AS NVARCHAR));
        PRINT('An error occurred: ' + ERROR_MESSAGE());
        PRINT('==================================================');
    END CATCH
END;
