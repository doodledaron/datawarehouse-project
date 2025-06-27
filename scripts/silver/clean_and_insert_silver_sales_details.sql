-- Since we want to join sales details with customer info and product info tales
-- Step 1: Check if the customer id in sales details are all in crm_cust_info
SELECT * from bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- Step 2: Check if the product id in sales details are all in crm_prd_info
SELECT * from bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- change the integers dates into date
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')  -- safer conversion
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')  -- safer conversion
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')  -- safer conversion
    END AS sls_due_dt,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details;

-- Check: order date should < ship date < due date -> ALL GOOD
SELECT * from bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Check Data consistency: Business Rules => Total sales = quantity x price
-- Negative, zeros and nulls are not allowed
-- Fix data quality issues:
-- sls_sales != quantity × price
-- Null/negative price → set to 0 or abs(price)
-- Keep quantity as-is
-- Recalculate sls_sales = quantity × price

SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE 
    sls_sales IS NULL OR
    sls_quantity IS NULL OR
    sls_price IS NULL OR
    sls_sales <= 0 OR
    sls_quantity <= 0 OR
    sls_price <= 0 OR
    sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price


-- UPDATE the sales based on the fixes
-- Fixes:
-- - Convert 0/invalid dates to NULL
-- - If sls_price is NULL, set to 0
-- - If sls_price is negative, convert to absolute value
-- - Recalculate sls_sales = sls_quantity * sls_price

SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Fix sls_order_dt
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END AS sls_order_dt,

    -- Fix sls_ship_dt
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END AS sls_ship_dt,

    -- Fix sls_due_dt
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END AS sls_due_dt,

    sls_quantity,

    -- Fix price: replace NULL with 0, make negative prices positive
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END AS sls_price,

    -- Recalculate sales
    sls_quantity * 
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END AS sls_sales

FROM bronze.crm_sales_details;


-- Drop and update table, then insert into silver layer
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
	sls_quantity INT,
	sls_price INT,
	sls_sales INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- System-generated timestamp
);


INSERT INTO silver.crm_sales_details(
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

    -- Fix sls_order_dt
    CASE 
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END AS sls_order_dt,

    -- Fix sls_ship_dt
    CASE 
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END AS sls_ship_dt,

    -- Fix sls_due_dt
    CASE 
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END AS sls_due_dt,

    sls_quantity,

    -- Fix price: replace NULL with 0, make negative prices positive
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END AS sls_price,

    -- Recalculate sales
    sls_quantity * 
    CASE 
        WHEN sls_price IS NULL THEN 0
        WHEN sls_price < 0 THEN ABS(sls_price)
        ELSE sls_price
    END AS sls_sales

FROM bronze.crm_sales_details;









