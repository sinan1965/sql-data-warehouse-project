/* 
============================================================
Stored Porcedure : Load Bronze Layer (Source -> Bronze)
============================================================
Script Purpose: 
    This stored procedure loads data into the Bronze schema from external csv files.
    It performs the following actions:
    - Truncates the Bronze tables before loading data.
    - Uses 'BULK INSERT' command to load data from cav files to Bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
=============================================================
*/
create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '=====================================================';
		print 'Laoding Bronze Layer';
		print '=====================================================';

		print '-----------------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print 'Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print 'Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print 'Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		print '-----------------------------------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------------------------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print 'Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print 'Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		set @start_time = getdate();
		print 'Truncating table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		print 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Dell\Desktop\sc genel\python\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		set @end_time = getdate();
		print '>> Load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '---------------';

		set @batch_end_time = GETDATE();

		print '=====================================================';
		print 'Loading Bronze Layer is completed...';
		print '  -Total Load Duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar) + ' seconds';
		print '=====================================================';

	end try
	begin catch
		print '=====================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		print 'Error Message' + error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '=====================================================';
	end catch
end

