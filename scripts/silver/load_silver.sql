-- ================================================
-- File: load_silver.sql
-- Purpose: Load cleaned and standardized data from bronze to silver layer
-- Action: TRUNCATE silver tables before insert
-- ================================================

CREATE OR REPLACE PROCEDURE load_silver()
LANGUAGE plpgsql
AS $$
BEGIN

-- ==========================================================
-- Step 0: TRUNCATE all silver tables in correct dependency order
-- ==========================================================
TRUNCATE TABLE 
    silver.crm_sales_details,
    silver.crm_prd_info,
    silver.crm_cust_info,
    silver.erp_cust_az12,
    silver.erp_loc_a101,
    silver.erp_px_cat_g1v2
RESTART IDENTITY CASCADE;

-- ========================================
-- Step 1: Insert into CRM Sales Details
-- ========================================
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_quantity,
    sls_price,
    sls_sales
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END,
    sls_quantity * 
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;

-- ========================================
-- Step 2: Insert into CRM Product Info
-- ========================================
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) - 1
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY prd_id
               ORDER BY prd_start_dt DESC
           ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;

-- ========================================
-- Step 3: Insert into CRM Customer Info
-- ========================================
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gnder,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) AS flag_last
    FROM bronze.crm_cust_info
) AS deduped
WHERE flag_last = 1;

-- ========================================
-- Step 4: Insert into ERP Customer Info
-- ========================================
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
    END,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;

-- ========================================
-- Step 5: Insert into ERP Location Info
-- ========================================
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', ''),
    CASE
        WHEN cntry IN ('US', 'United States') THEN 'United States'
        WHEN cntry = 'DE' THEN 'Germany'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
        ELSE cntry
    END
FROM bronze.erp_loc_a101;

-- ========================================
-- Step 6: Insert into ERP PX Category
-- ========================================
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

END;
$$;

-- To run the procedure:
CALL load_silver();
