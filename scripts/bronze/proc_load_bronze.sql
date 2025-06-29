-- ================================================
-- Procedure: bronze.load_bronze()
-- Purpose: Import data from CSVs into bronze schema
-- Action: Truncate before insert to ensure idempotency
-- ================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ========================================
    -- CRM Customer Information
    -- ========================================
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Library/PostgreSQL/17/data/csvs/cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- ========================================
    -- CRM Product Information
    -- ========================================
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Library/PostgreSQL/17/data/csvs/prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- ========================================
    -- CRM Sales Details
    -- ========================================
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Library/PostgreSQL/17/data/csvs/sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- ========================================
    -- ERP Customer AZ12
    -- ========================================
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Library/PostgreSQL/17/data/csvs/cust_az12.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- ========================================
    -- ERP Location A101
    -- ========================================
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Library/PostgreSQL/17/data/csvs/loc_a101.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    -- ========================================
    -- ERP PX Category G1V2
    -- ========================================
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Library/PostgreSQL/17/data/csvs/px_cat_g1v2.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
END;
$$;
