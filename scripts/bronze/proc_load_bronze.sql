/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE or Alter Procedure bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY
		SET @batch_start_time = GETDATE();
        PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';
    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.crm_cust_info';
    Truncate Table bronze.crm_cust_info;
    PRINT '>> Inserting Data Into: bronze.crm_cust_info';
    Bulk insert bronze.crm_cust_info
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
    );
       SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();
		 

    PRINT '>> Truncating Table: bronze.crm_prd_info';
    Truncate Table bronze.crm_prd_info;
    PRINT '>> Inserting Data Into: bronze.crm_prd_info';
    Bulk insert bronze.crm_prd_info
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
    );  
       
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        SET @start_time = GETDATE();

    PRINT '>> Truncating Table: bronze.crm_sales_details';
    Truncate Table bronze.crm_sales_details;
    PRINT '>> Inserting Data Into: bronze.crm_sales_details';
    Bulk insert bronze.crm_sales_details
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
    );

      SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		
		SET @start_time = GETDATE();  

    PRINT '>> Truncating Table: bronze.erp_loc_a101';
    Truncate Table bronze.erp_loc_a101;
    PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
    Bulk insert bronze.erp_loc_a101
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
    ); 
     SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE()  
    PRINT '>> Truncating Table: bronze.erp_cust_az12';
    Truncate Table bronze.erp_cust_az12;
    PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
    Bulk insert bronze.erp_cust_az12
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
    );
      SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @start_time = GETDATE();
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
    Truncate Table bronze.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    Bulk insert bronze.erp_px_cat_g1v2
    from 'E:\SQL\Project 1 Data Warehouse\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
    with (
          Firstrow = 2,
          FIELDTERMINATOR = ',',
          TABLOCK
);
 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END
