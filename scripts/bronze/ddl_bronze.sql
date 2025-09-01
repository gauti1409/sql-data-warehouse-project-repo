/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

create table if not exists bronze.crm_cust_inf(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_material_status varchar(50),
cst_gndr varchar(50),
cst_create_date date 
);

create table if not exists bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_name varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date 
);

create table if not exists bronze.crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_date int,
sls_ship_date int,
sls_due_date int,
sls_sales int,
sls_quantity int,
sls_price int
);


create table if not exists bronze.erp_loc_a101 (
    cid    varchar(50),
    cntry  varchar(50)
);



create table if not exists bronze.erp_cust_az12 (
    cid    varchar(50),
    bdate  date,
    gen    varchar(50)
);


create table if not exists bronze.erp_px_cat_g1v2 (
    id           varchar(50),
    cat          varchar(50),
    subcat       varchar(50),
    maintenance  varchar(50)
);
