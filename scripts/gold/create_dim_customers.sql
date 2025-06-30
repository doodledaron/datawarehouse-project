-- Join all customer tables
-- Master table: CRM_CUST INFO
-- Joining strategy: Preserve all the info in crm_cust_info (master table)
-- Use left join: Left table: crm_cust_info (all info preserved)
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gnder,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
-- from is the MASTER table (all rows will be preserved)
FROM silver.crm_cust_info ci
-- join customer details
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
-- join customer location details
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- We have cst_gnder and gen coloumn which means the same thing
-- to solve this problem: Data Integration
-- Step 1: do checking
SELECT DISTINCT
    ci.cst_gnder,   -- this is column 1
    ca.gen          -- this is column 2
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid
ORDER BY 1,2 -- order by column 1, then column 2

-- Step 2: Data integration
-- Since gen is from erp system, and cst_gnder is crm system (master table) -> we now assume crm system is the single source of truth
SELECT DISTINCT
    ci.cst_gnder,   
    ca.gen,
	CASE 
		WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder --if cst_gnder is not 'n/a' → use it directly
		ELSE COALESCE(ca.gen, 'n/a') -- else use ca.gen, BUT if ca.gen is null, fallback to 'n/a'
	END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid
ORDER BY 1,2


-- Step 3: Data integration into the full table
SELECT 
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_key,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder --if cst_gnder is not 'n/a' → use it directly
		ELSE COALESCE(ca.gen, 'n/a') -- else use ca.gen, BUT if ca.gen is null, fallback to 'n/a'
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
-- from is the MASTER table (all rows will be preserved)
FROM silver.crm_cust_info ci
-- join customer details
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
-- join customer location details
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- Step 4: Generate surrogate key
-- system generated unique identifier assignend to each record in a table
-- straightforward way: use window functions
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder --if cst_gnder is not 'n/a' → use it directly
		ELSE COALESCE(ca.gen, 'n/a') -- else use ca.gen, BUT if ca.gen is null, fallback to 'n/a'
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
-- from is the MASTER table (all rows will be preserved)
FROM silver.crm_cust_info ci
-- join customer details
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
-- join customer location details
LEFT JOIN silver.erp_loc_a101 la
on ci.cst_key = la.cid


-- Step 5: Create View -> virtual table
DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,  -- surrogate key
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gnder != 'n/a' THEN ci.cst_gnder
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;

-- the view will now be at the 'views' tab in gold layer
SELECT * from gold.dim_customers