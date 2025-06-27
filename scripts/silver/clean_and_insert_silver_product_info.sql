SELECT * from bronze.crm_prd_info;

-- Step 1 Check for nulls or duplicates in primary key
SELECT 
prd_id,
COUNT(*)
from bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

-- Remove duplicate product ID
SELECT 
    prd_id,
	prd_key,
    prd_nm,
	prd_cost,
	prd_line,
    prd_start_dt
	prd_end_dt
FROM(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY prd_id
ORDER BY prd_start_dt DESC
) as flag_last
FROM bronze.crm_prd_info
) WHERE flag_last = 1;

-- Step 2 Data Normalization/Standardization
-- Extract product key substring -> cat_id
-- replace '-' of cat_id to '_'
-- extract product key for sales setails
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_start_dt DESC
        ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;

-- check for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info 
where prd_nm != TRIM(prd_nm)

--check for NULLS or Negative numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--replace the null with 0
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
	-- if its null -> 0, else use the actual value
    COALESCE(prd_cost, 0) AS prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_start_dt DESC
        ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;

-- check for distinct values of product line
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Replace the low cardinality columns to meaningful values
SELECT 
    prd_id,
    prd_key,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
	-- if its null -> 0, else use the actual value
    COALESCE(prd_cost, 0) AS prd_cost,
	-- replace prd_line with meaningful values
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    prd_end_dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_start_dt DESC
        ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;

-- Check for invalid date orders -> End date must not be earlier than the start date
SELECT * from bronze.crm_prd_info WHERE prd_end_dt < prd_start_dt

-- Issue:
-- 1. end date is earlier than start date
-- 2. even when we switch end date and start date, there will be overlapping of data

-- solution:
-- the end date of that row == (start date of next row) - 1
-- this shows the increase of product cost

-- Check for two products first
SELECT
prd_id, 
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

-- Apply LEAD() Function, window function over the next row
-- Check for two products first
SELECT
prd_id, 
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) -1 AS test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- Apply in the full function -> cleaned
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    prd_nm,
	-- if its null -> 0, else use the actual value
    COALESCE(prd_cost, 0) AS prd_cost,
	-- replace prd_line with meaningful values
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) -1 AS prd_end_dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_start_dt DESC
        ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;


-- drop and update the table after data transformation from the bronze layer
DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
	cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- System-generated timestamp
);

-- INSERT the cleaned bronze prd info
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
	-- if its null -> 0, else use the actual value
    COALESCE(prd_cost, 0) AS prd_cost,
	-- replace prd_line with meaningful values
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
	-- data enrichment -> enchance value for data analysis
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_end_dt) -1 AS prd_end_dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_start_dt DESC
        ) AS flag_last
    FROM bronze.crm_prd_info
) AS cleaned
WHERE flag_last = 1;

SELECT * from silver.crm_prd_info


