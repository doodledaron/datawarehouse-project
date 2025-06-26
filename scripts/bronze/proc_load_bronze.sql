-- =====================================================
-- 📌 BULK INSERT INTO TABLES FROM CSV (PostgreSQL)
-- =====================================================

-- ✅ METHOD 1: Using psql CLI
-- Run this script in terminal with:
--    psql -U postgres -d DataWarehouse -f import_data.sql
-- Make sure the file paths below point to your LOCAL machine.
-- =====================================================

-- CRM Customer Info
\COPY bronze.crm_cust_info
FROM '/path/to/your/csvs/cust_info.csv'
WITH (
    FORMAT csv,       -- The file is in CSV format
    HEADER true,      -- Skip header row
    DELIMITER ','     -- Column separator
);

-- CRM Product Info
\COPY bronze.crm_prd_info
FROM '/path/to/your/csvs/prd_info.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);

-- CRM Sales Details
\COPY bronze.crm_sales_details
FROM '/path/to/your/csvs/sales_details.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);

-- ERP Customer AZ12
\COPY bronze.erp_cust_az12
FROM '/path/to/your/csvs/cust_az12.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);

-- ERP Location A101
\COPY bronze.erp_loc_a101
FROM '/path/to/your/csvs/loc_a101.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);

-- ERP PX Category G1V2
\COPY bronze.erp_px_cat_g1v2
FROM '/path/to/your/csvs/px_cat_g1v2.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ','
);


-- =====================================================
-- 🖥️ METHOD 2: Using pgAdmin GUI (no SQL needed)
-- =====================================================
-- 1. In pgAdmin, right-click on any target table (e.g. crm_cust_info)
-- 2. Select: Import/Export
-- 3. Choose:
--    - Filename: Browse to your .csv file
--    - Format: CSV
--    - Header: ✅ Check "Header"
--    - Delimiter: ,
-- 4. Click "OK" to load the data
-- Repeat this for each table and its respective file
-- =====================================================

