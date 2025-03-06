/*
===============================================================================
Quality Checks for Silver Layer
===============================================================================
Script Purpose:
    This script performs a series of quality checks on the 'silver' layer to ensure 
    data consistency, accuracy, and standardization. The checks include:
    - Validation of primary keys (nulls or duplicates).
    - Removal of unwanted spaces in string fields.
    - Standardization and consistency of categorical data.
    - Validation of date ranges and logical ordering.
    - Consistency between related fields (e.g., sales = quantity * price).

Usage Notes:
    - Run these checks after loading data into the silver layer.
    - Investigate and resolve any discrepancies identified during the checks.
===============================================================================
*/

-- ====================================================================
-- Quality Checks for 'silver.crm_cust_info'
-- ====================================================================

-- Check for NULL or Duplicate Primary Keys
-- Expectation: No results should be returned.
PRINT('Checking for NULL or Duplicate Primary Keys in silver.crm_cust_info...');
SELECT 
    cst_id,
    COUNT(*) AS record_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in String Fields
-- Expectation: No results should be returned.
PRINT('Checking for Unwanted Spaces in silver.crm_cust_info...');
SELECT 
    cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Validate Data Standardization for Marital Status
-- Expectation: Only valid values ('Single', 'Married', 'n/a') should exist.
PRINT('Validating Marital Status Standardization in silver.crm_cust_info...');
SELECT DISTINCT 
    cst_marital_status
FROM silver.crm_cust_info;

-- Validate Data Standardization for Gender
-- Expectation: Only valid values ('Male', 'Female', 'n/a') should exist.
PRINT('Validating Gender Standardization in silver.crm_cust_info...');
SELECT DISTINCT 
    cst_gender
FROM silver.crm_cust_info;

-- ====================================================================
-- Quality Checks for 'silver.crm_prd_info'
-- ====================================================================

-- Check for NULL or Duplicate Primary Keys
-- Expectation: No results should be returned.
PRINT('Checking for NULL or Duplicate Primary Keys in silver.crm_prd_info...');
SELECT 
    prd_id,
    COUNT(*) AS record_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces in Product Names
-- Expectation: No results should be returned.
PRINT('Checking for Unwanted Spaces in silver.crm_prd_info...');
SELECT 
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Validate Product Cost Values
-- Expectation: No negative or null values should exist.
PRINT('Validating Product Costs in silver.crm_prd_info...');
SELECT 
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Validate Data Standardization for Product Line
-- Expectation: Only valid values ('Other Sales', 'Mountain', 'Road', 'Touring', 'n/a') should exist.
PRINT('Validating Product Line Standardization in silver.crm_prd_info...');
SELECT DISTINCT 
    prd_line
FROM silver.crm_prd_info;

-- Validate Date Ordering (Start Date <= End Date)
-- Expectation: No invalid date orders should exist.
PRINT('Validating Date Ordering in silver.crm_prd_info...');
SELECT 
    *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Quality Checks for 'silver.crm_sales_details'
-- ====================================================================

-- Validate Date Ranges
-- Expectation: All dates should fall within valid ranges.
PRINT('Validating Date Ranges in silver.crm_sales_details...');
SELECT 
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > '2050-01-01' 
   OR sls_order_dt < '1900-01-01'
   OR sls_ship_dt > '2050-01-01' 
   OR sls_ship_dt < '1900-01-01'
   OR sls_due_dt > '2050-01-01' 
   OR sls_due_dt < '1900-01-01';

-- Validate Logical Date Ordering (Order Date <= Ship Date <= Due Date)
-- Expectation: No invalid date orders should exist.
PRINT('Validating Logical Date Ordering in silver.crm_sales_details...');
SELECT 
    *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt
   OR sls_ship_dt > sls_due_dt;

-- Validate Sales Calculation (Sales = Quantity * Price)
-- Expectation: No inconsistencies should exist.
PRINT('Validating Sales Calculation in silver.crm_sales_details...');
SELECT 
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0;

-- ====================================================================
-- Quality Checks for 'silver.erp_cust_az12'
-- ====================================================================

-- Validate Birthdate Range
-- Expectation: Birthdates should fall between 1924-01-01 and today.
PRINT('Validating Birthdate Range in silver.erp_cust_az12...');
SELECT 
    cid,
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Validate Gender Standardization
-- Expectation: Only valid values ('Male', 'Female', 'n/a') should exist.
PRINT('Validating Gender Standardization in silver.erp_cust_az12...');
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12;

-- ====================================================================
-- Quality Checks for 'silver.erp_loc_a101'
-- ====================================================================

-- Validate Country Standardization
-- Expectation: Only valid country names should exist.
PRINT('Validating Country Standardization in silver.erp_loc_a101...');
SELECT DISTINCT 
    cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Quality Checks for 'silver.erp_px_cat_g1v2'
-- ====================================================================

-- Check for Unwanted Spaces in String Fields
-- Expectation: No results should be returned.
PRINT('Checking for Unwanted Spaces in silver.erp_px_cat_g1v2...');
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Validate Maintenance Field Standardization
-- Expectation: Only valid values should exist.
PRINT('Validating Maintenance Field Standardization in silver.erp_px_cat_g1v2...');
SELECT DISTINCT 
    maintenance
FROM silver.erp_px_cat_g1v2;
