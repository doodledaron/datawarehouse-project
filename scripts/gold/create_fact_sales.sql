-- Create fact sales
SELECT * from silver.crm_sales_details

SELECT * from gold.dim_products
SELECT * from gold.dim_customers
-- Master table: crm_sales_details
-- We have to join with gold.dim_customers and gold.dim_products to get their surrogate key
-- INtead of sls_prd_key and sls_cust_id, we need surrogate key from both dimensin tables
-- then only we could form the gold layer
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number, -- dimension keys
	pr.product_key, -- dimension keys (surrogate key)
	ct.customer_key, -- dimension keys (surrogate key)
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers ct
ON sd.sls_cust_id = ct.customer_id