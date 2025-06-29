SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

-- id is good to use, will be linked to the product key
-- check unwanted whitespaces in cat, subcat, and maintenance -> all good
SELECT * from bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

-- Data Standardization 
-- CHeck the Unique values for all columns
SELECT DISTINCT 
maintenance 
from bronze.erp_px_cat_g1v2

-- INSERT INTO SILVER LAYER
INSERT INTO silver.erp_px_cat_g1v2(
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
FROM bronze.erp_px_cat_g1v2



