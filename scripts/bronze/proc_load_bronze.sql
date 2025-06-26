-- ==========================================
-- 🚀 Stored Procedure: import_crm_data
-- Purpose: Bulk import multiple CSV files into their corresponding tables
-- Requirements:
--   ✅ All CSV files must be stored on the PostgreSQL server's file system
--   ❌ This will NOT work if the files are on your local machine (e.g., Mac) and you're using COPY
--   ✅ File paths must be absolute and accessible by the PostgreSQL server process
-- ==========================================

CREATE OR REPLACE PROCEDURE bronze.import_crm_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- 📁 Import CRM Customer Information
    -- File: cust_info.csv must exist in server directory: /Library/PostgreSQL/17/data/csvs/
    COPY bronze.crm_cust_info
    FROM '/Library/PostgreSQL/17/data/csvs/cust_info.csv'
    WITH (
        FORMAT csv,       -- File format
        HEADER true,      -- Skip the header row
        DELIMITER ','     -- Use comma as field separator
    );

    -- 📁 Import CRM Product Information
    COPY bronze.crm_prd_info
    FROM '/Library/PostgreSQL/17/data/csvs/prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- 📁 Import CRM Sales Details
    COPY bronze.crm_sales_details
    FROM '/Library/PostgreSQL/17/data/csvs/sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- 📁 Import ERP Customer AZ12
    COPY bronze.erp_cust_az12
    FROM '/Library/PostgreSQL/17/data/csvs/cust_az12.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- 📁 Import ERP Location A101
    COPY bronze.erp_loc_a101
    FROM '/Library/PostgreSQL/17/data/csvs/loc_a101.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- 📁 Import ERP PX Category G1V2
    COPY bronze.erp_px_cat_g1v2
    FROM '/Library/PostgreSQL/17/data/csvs/px_cat_g1v2.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
END;
$$;

-- ==========================================
-- 🟢 Execute the procedure
-- Run this from pgAdmin Query Tool or psql CLI:
CALL bronze.import_crm_data();
-- ==========================================
