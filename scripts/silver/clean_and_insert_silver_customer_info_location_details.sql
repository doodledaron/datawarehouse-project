SELECT 
cid,
cntry
FROM bronze.erp_loc_a101

-- data standardization, replace '-' with ''
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101

-- check all customer key is in silver crm cust info -> all good
SELECT 
REPLACE(cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN 
(
	SELECT cst_key from silver.crm_cust_info


-- check the countries
SELECT DISTINCT
cntry 
FROM bronze.erp_loc_a101

-- Data standardization for country
SELECT
REPLACE(cid, '-', '') cid,
CASE
	WHEN cntry IN ('US', 'United States') THEN 'United States'
	WHEN cntry = 'DE' THEN 'Germany'
	WHEN cntry IS NULL OR TRIM(cntry)= '' THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

-- Insert into silver layer
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
REPLACE(cid, '-', '') cid,
CASE
	WHEN cntry IN ('US', 'United States') THEN 'United States'
	WHEN cntry = 'DE' THEN 'Germany'
	WHEN cntry IS NULL OR TRIM(cntry)= '' THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

