/*

 Stored Procedure: Load Bronze Layer (Source -> Bronze)
 
 Script Purpose:
 	This stored procedure loads data in to the 'bronze' schema from external CSV files.
 	It: 
 		- Truncates the bronze tables before loading data
 		- Uses 'BULK INSERT' to load data from local csv files to bronze tables
 
 Usage Example:
 	EXEC bronze.load_bronze;



 */


use DataWarehouse;

CREATE or ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @bronze_start_time DATETIME;
	DECLARE @start_time DATETIME , @end_time DATETIME;

	BEGIN TRY 
	
		SET @bronze_start_time = GETDATE();
		
		PRINT '====================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================';
	
		PRINT '------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------';
		
		SET @start_time = GETDATE();
	
		PRINT '>> TRUNCATE and INSERT bronze.crm_cust_info';
		TRUNCATE table bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM '/var/opt/mssql/import/source_crm/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		SET @start_time = GETDATE();
		
		PRINT '>> TRUNCATE and INSERT bronze.crm_prd_info';
		TRUNCATE table bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM '/var/opt/mssql/import/source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
				
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		
		SET @start_time = GETDATE();
		
		PRINT '>> TRUNCATE and INSERT bronze.crm_sales_details';
		TRUNCATE table bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM '/var/opt/mssql/import/source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		PRINT '------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------';
	
		
		SET @start_time = GETDATE();
		
		PRINT '>> TRUNCATE and INSERT bronze.erp_cust_az12';
		TRUNCATE table bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM '/var/opt/mssql/import/source_erp/cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
						
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		
		SET @start_time = GETDATE();
		
		PRINT '>> TRUNCATE and INSERT bronze.erp_loc_a101';
		TRUNCATE table bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM '/var/opt/mssql/import/source_erp/loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
						
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		
		SET @start_time = GETDATE();
		
		PRINT '>> TRUNCATE and INSERT bronze.erp_px_cat_g1v2';
		TRUNCATE table bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/var/opt/mssql/import/source_erp/px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
						
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		
		PRINT '======================================================'
		PRINT '>> Full Bronze Layer Load Duration: ' + CAST(DATEDIFF(second, @bronze_start_time, @end_time) AS NVARCHAR) + ' seconds';
		
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================';
		
	END CATCH
END
