--- normalize data
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- Check if all customer info in this table is available on the silver table customer info
SELECT 
CASE 
	-- normalize the cid
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
	--normalize the cid not in the selected distict cst key from silver
	WHERE 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END 
		NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- Identify out-of-range birthdates
SELECT DISTINCT 
  bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;

-- clean the unrealistic birthdates
-- Our approach for now, clean the extremes, which are the future
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12

-- check for gender values
SELECT DISTINCT 
gen 
FROM bronze.erp_cust_az12

-- Data Normalization and Data Standardization for gen
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END AS bdate,
CASE
	  WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	  ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


-- INSERT INTO silver later
INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > CURRENT_DATE THEN NULL
	ELSE bdate
END AS bdate,
CASE
	  WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	  ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


