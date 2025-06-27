-- just to view the info 29466
SELECT * 
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- step 1: remove duplicates
--reordered row by row based on the cst_create_date, we want to find the latest create date, then select only the latest date, which remove duplicate users
SELECT 
*
FROM (
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
) WHERE flag_last = 1
AND cst_id = 11233;

--step 2 check and remove unwanted spaces
-- check for unwanted spaces -> first name and last name
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- remove the unwanted spaces + remove duplicates
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
cst_marital_status,
cst_gnder,
cst_create_date
FROM (
-- from the previous remove duplication query
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
) WHERE flag_last = 1;


-- Step 3:  Check for data consistency for low cardinality columns
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

-- Change the low cardinallity columns to meaningful values
-- UPPER: make sure all values are changed to upper case
-- TRIM: make sure all values are trimmed (no whitespaces)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
-- Case 1: Marital Status
CASE 
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
ELSE 'n/a'
END cst_marital_status,
-- Case 2: Gender
CASE 
WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female'
WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
ELSE 'n/a'
END cst_gnder,
cst_create_date
FROM (
-- from the previous remove duplication query
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
) WHERE flag_last = 1;


-- Step 4: Insert into silver layer
INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gnder,
cst_create_date
)
-- Change the low cardinallity columns to meaningful values
-- UPPER: make sure all values are changed to upper case
-- TRIM: make sure all values are trimmed (no whitespaces)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
-- Case 1: Marital Status
CASE 
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
ELSE 'n/a'
END cst_marital_status,
-- Case 2: Gender
CASE 
WHEN UPPER(TRIM(cst_gnder)) = 'F' THEN 'Female'
WHEN UPPER(TRIM(cst_gnder)) = 'M' THEN 'Male'
ELSE 'n/a'
END cst_gnder,
cst_create_date
FROM (
-- from the previous remove duplication query
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
) WHERE flag_last = 1;
