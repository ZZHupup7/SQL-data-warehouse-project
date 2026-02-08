/*
===============================================================================
Script Name: Create Bronze Layer Tables
Description: 
    This script initializes the 'bronze' schema by creating tables for 
    Source CRM and ERP systems. These tables serve as the entry point 
    for raw data ingestion into the Data Warehouse.
    
Architecture: Medallion (Bronze -> Silver -> Gold)
===============================================================================
*/

-------------------------------------------------------------------------------
-- 1. CRM Source Tables
-------------------------------------------------------------------------------

-- Create 'bronze.crm_cust_info' to store raw customer profiles
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_creat_date     DATE
);

-- Create 'bronze.crm_prd_info' to store product master data
IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- Create 'bronze.crm_sales_details' to store transaction records
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-------------------------------------------------------------------------------
-- 2. ERP Source Tables
-------------------------------------------------------------------------------

-- Create 'bronze.erp_loc_a101' for raw location/country mapping
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);

-- Create 'bronze.erp_cust_az12' for supplementary customer demographics
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);

-- Create 'bronze.erp_px_cat_g1v2' for product categorization and maintenance
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
