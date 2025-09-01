/*===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    call bronze.load_bronze_tables();
===============================================================================
*/

create or replace procedure bronze.load_bronze_tables() 
language plpgsql
as $$
DECLARE
	-- This variable will hold the number of rows affected by the last command.
	v_rows_copied BIGINT;
	-- Variables for capturing error details
	v_error_message TEXT;
	v_error_state TEXT;

BEGIN  
	BEGIN  

		/* FOR bronze.crm_cust_inf */
		
	  RAISE NOTICE '==================================================================================================';
		RAISE NOTICE 'Loading Bronze layer started at %', now();
		RAISE NOTICE '==================================================================================================';
	
		RAISE NOTICE '---------------------------------------------------------------------------------------------------';
	  RAISE NOTICE 'Loading CRM Tables started at %', now();
	  RAISE NOTICE '---------------------------------------------------------------------------------------------------';
	
		
		RAISE NOTICE 'Truncating Table : bronze.crm_cust_inf';
		truncate table bronze.crm_cust_inf;
		
	  RAISE NOTICE 'Inserting Data Into : bronze.crm_cust_inf';
		COPY bronze.crm_cust_inf (cst_id, cst_key , cst_firstname , cst_lastname , cst_material_status , cst_gndr , cst_create_date )
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ',' 
		CSV HEADER; 
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.crm_cust_inf;
		RAISE NOTICE 'COPY successful. % rows copied. ', v_rows_copied;
		
		
		/* FOR bronze.crm_prd_info */
		RAISE NOTICE 'Truncating Table : bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		
	  RAISE NOTICE 'Inserting Data Into : bronze.crm_prd_info';
		COPY bronze.crm_prd_info (prd_id, prd_key , prd_name , prd_cost , prd_line , prd_start_dt , prd_end_dt)
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ',' 
		CSV HEADER; 
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.crm_prd_info;
		RAISE NOTICE 'COPY successful. % rows copied. ', v_rows_copied;
		
		
		/* FOR bronze.crm_sales_details */
		RAISE NOTICE 'Truncating Table : bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		
	  RAISE NOTICE 'Inserting Data Into : bronze.crm_sales_details';
		COPY bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id , sls_order_date , sls_ship_date , sls_due_date , sls_sales ,
		                               sls_quantity , sls_price )
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ',' 
		CSV HEADER; 
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.crm_sales_details;
		RAISE NOTICE 'COPY successful. % rows copied.', v_rows_copied;
	
		
		RAISE NOTICE '---------------------------------------------------------------------------------------------------' ;
	  RAISE NOTICE 'Loading ERP Tables started at %', now();
	  RAISE NOTICE '---------------------------------------------------------------------------------------------------' ;
		
		
		/* FOR bronze.erp_cust_az12 */
		RAISE NOTICE 'Truncating Table : bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
	
		RAISE NOTICE 'Inserting Data Into : bronze.erp_cust_az12';	
		COPY bronze.erp_cust_az12 (cid, bdate, gen)
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		DELIMITER ',' 
		CSV HEADER; 
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.erp_cust_az12;
		RAISE NOTICE 'COPY successful. % rows copied.', v_rows_copied;
		
		
		
		/* FOR bronze.erp_loc_a101 */
		RAISE NOTICE 'Truncating Table : bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		
	  RAISE NOTICE 'Inserting Data Into : bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101 (cid, cntry)
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		DELIMITER ',' 
		CSV HEADER; 
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.erp_loc_a101;
		RAISE NOTICE 'COPY successful. % rows copied.', v_rows_copied;
		
		
		/* FOR bronze.erp_px_cat_g1v2 */
		RAISE NOTICE 'Truncating Table : bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		
	  RAISE NOTICE 'Inserting Data Into : bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		FROM 'E:\Personal_Documents\Big_Data_by_Trendytech\Interview_Readiness\SQL\SQL_DataWareHouse_project_by_Baraa\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		DELIMITER ',' 
		CSV HEADER;
	
	    -- Get the number of rows copied from the last command
		SELECT COUNT(*) INTO v_rows_copied FROM bronze.erp_px_cat_g1v2;
		RAISE NOTICE 'COPY successful. % rows copied.', v_rows_copied;

	EXCEPTION
		-- This single block catches any error that occurs within the main BEGIN block
		WHEN OTHERS THEN
			GET STACKED DIAGNOSTICS
				v_error_message = MESSAGE_TEXT,
				v_error_state = PG_EXCEPTION_CONTEXT;
			RAISE EXCEPTION E'Data loading failed: %', v_error_message;
	END;

END;
$$;






