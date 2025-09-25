
/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
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
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;


Additional Checks You Can Add
1. Referential Integrity (Cross-Table Checks)

Ensure foreign key relationships hold true:

-- Example: Every sales record must have a valid customer
SELECT sls_id
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c ON s.cst_id = c.cst_id
WHERE c.cst_id IS NULL;


(Similarly, check product IDs in crm_sales_details exist in crm_prd_info.)

2. Allowed Values / Domain Checks

Ensure categorical columns only contain valid domain values (beyond just DISTINCT):

-- Example: Gender column should only have 'M','F','O'
SELECT DISTINCT gen 
FROM silver.erp_cust_az12
WHERE gen NOT IN ('M','F','O');


(Same for cst_marital_status, prd_line, maintenance etc. → should match your business dictionary.)

3. Email / Phone / ID Format Checks

If you store customer info:

-- Check valid email pattern
SELECT cst_email
FROM silver.crm_cust_info
WHERE cst_email NOT LIKE '%_@__%.__%';

-- Phone number length/format
SELECT phone
FROM silver.crm_cust_info
WHERE LEN(phone) NOT BETWEEN 10 AND 15;

4. Numeric Range Validations

Already checked negative costs, but you can also check:

Unrealistic ages (from bdate).

Product costs too high (business limit check).

Sales totals not exceeding a threshold (sanity check).

5. Duplicate Detection Beyond Primary Keys

Duplicate rows where all fields match (not just PKs).

SELECT *, COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
HAVING COUNT(*) > 1;

6. Consistency Across Fields

Cross-column logical checks:

If prd_end_dt IS NULL, product should still be active.

If cst_marital_status = 'Married', spouse_name should not be NULL (if available).

If sls_quantity = 0, sales should also be 0.

7. Timeliness Checks

Ensure data is up-to-date:

-- Expect no order dates from the future
SELECT * 
FROM silver.crm_sales_details
WHERE sls_order_dt > GETDATE();

8. Outlier Detection

Identify extreme values (statistical check, not just rule-based):

-- Flag sales much higher than usual
SELECT * 
FROM silver.crm_sales_details
WHERE sls_sales > (SELECT AVG(sls_sales)*10 FROM silver.crm_sales_details);

9. Standard Naming Conventions

Check for consistency in codes/IDs (e.g., country codes in erp_loc_a101 should be ISO-2 or ISO-3 format).

SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
WHERE LEN(cntry) NOT IN (2,3);

10. Null Coverage Reports

Instead of one-off NULL checks, generate a null profile for all columns:

SELECT 
    COLUMN_NAME, 
    SUM(CASE WHEN COLUMN_VALUE IS NULL THEN 1 ELSE 0 END) AS null_count
FROM silver.crm_cust_info
GROUP BY COLUMN_NAME;
