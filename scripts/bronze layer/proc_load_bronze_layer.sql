exec bronze.load_bronze;



create or alter procedure bronze.load_bronze as
begin
begin try 
	print'============================================'
	print 'Loading the bronze layer'
	print'============================================'

	declare @start_time datetime ,@end_time datetime
	truncate table bronze.crm_cust_info
	set @start_time=getdate()
	bulk insert bronze.crm_cust_info 
	from "D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	); set @end_time=getdate()

	print'load duration : '+cast(datediff(second,@start_time,@end_time)as varchar)+'seconds'
	--select * from bronze.crm_cust_info
	--select count(*)  from bronze.crm_cust_info
	truncate table bronze.crm_prd_info
	bulk insert bronze.crm_cust_info 
	from "D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);

	truncate table bronze.crm_sales_details
	set @start_time=getdate()
	bulk insert bronze.crm_sales_details
	from "D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);set @end_time=getdate()

	print'load duration : '+cast(datediff(second,@start_time,@end_time)as varchar)+'seconds'

	truncate table bronze.erp_cust_az12
	set @start_time=getdate()
	bulk insert bronze.erp_cust_az12
	from "D:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);set @end_time=getdate()
	print'load duration : '+cast(datediff(second,@start_time,@end_time)as varchar)+'seconds'

	truncate table bronze.erp_loc_a101
	set @start_time=getdate()
	bulk insert bronze.erp_loc_a101
	from "D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);set @end_time=getdate()
	print'load duration : '+cast(datediff(second,@start_time,@end_time)as varchar)+'seconds'

	truncate table bronze.erp_px_cat_g1v2
	set @start_time=getdate()
	bulk insert bronze.erp_px_cat_g1v2
	from "D:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv"
	with(
		firstrow=2,
		fieldterminator=',',
		tablock
	);set @end_time=getdate()
	print'load duration : '+cast(datediff(second,@start_time,@end_time)as varchar)+'seconds'
	end try
	begin catch
	print'===================================================='
	print'error message'+ error_message()
	print'error number'+cast(error_number() as varchar);
	print'===================================================='
	end catch
end

