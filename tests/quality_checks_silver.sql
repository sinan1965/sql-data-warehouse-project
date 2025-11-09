/*
===================================================================
Quality Checks
===================================================================
Script Purpose:
  This script performs various quality checks for data consistency, 
  accuracy and standardization across the 'silver' schemas. 
  It includes checks for:
    - Null or duplicte primary keys.
    - Unwanted spaces in string fileds.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===================================================================
*/

--Check for Nulls and Duplicates in Primary Key
--expectation: no result

select cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

--transform

select *
from (
select *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
)x where flag_last != 1;

-----------------------------------------------

--Check for unwanted spaces in string values
--Expectation: no result

select cst_firstname from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname);

--transform

select
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname
from bronze.crm_cust_info;


--Check the consistency of values in low cardinality columns
select distinct(cst_gndr) from bronze.crm_cust_info;

select * from bronze.crm_cust_info;

--transform


TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer

select * from silver.crm_cust_info;

select cst_lastname
from silver.crm_cust_info
where cst_lastname != trim(cst_firstname);


------------------------------
--product info

select 
prd_id,
prd_key,
replace(substring (prd_key, 1,5), '-','_') as cat_id,
substring (prd_key, 7, len(prd_key)) as prd_key,
prd_name,
isnull (prd_cost, 0) as prd_cost,
case upper(trim(prd_line))
	when 'M' then 'Mountain'
	when 'R' then 'Road'
	when 'S' then 'Other sales'
	when 'T' then 'Touring'
	else 'n/a'
end as prd_line,
prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt 
from bronze.crm_prd_info

/*where substring (prd_key, 7, len(prd_key)) in
(select sls_prd_key from bronze.crm_sales_details)*/

select prd_name from bronze.crm_prd_info
where prd_name != trim(prd_name);

select prd_cost from bronze.crm_prd_info
where prd_cost <= 0 or prd_cost is null;

select distinct(prd_line) from bronze.crm_prd_info;

select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

select 
prd_id,
prd_key,
prd_name,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prd_end_dt 
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509','AC-HE-HL-U509-R');

select prd_id,
count(*) from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

select prd_name from silver.crm_prd_info
where prd_name != trim(prd_name);

select prd_cost from silver.crm_prd_info
where prd_cost <= 0 or prd_cost is null;

select distinct(prd_line) from silver.crm_prd_info;

select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * from silver.crm_prd_info;

--------------------------------------
--sales details

select top(5) *  from bronze.crm_sales_details;

insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
	else cast(cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
	else cast(cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
	else cast(cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_price is null or sls_price <=0   
	then sls_sales / nullif(sls_quantity,0)
	else sls_price
end as sls_price,
sls_quantity,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
	then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales
from bronze.crm_sales_details


/*
where sls_cust_id not in (select cst_id from silver.crm_cust_info);
*/

/*
where sls_prd_key not in (select prd_key from silver.crm_prd_info);
*/

/*
where sls_ord_num != trim(sls_ord_num) or sls_ord_num is null;
*/

--Check for invalid dates
select sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0 or sls_order_dt is null;

select 
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <=0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101;

--check id order date is smaller then ship and dur dates
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

--check if sales, quantity or price columns have 0, negative or null values.

select distinct 
sls_sales as old_sls_sales, 
sls_quantity as old_sls_quantity, 
sls_price as sls_old_price,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) 
	then sls_quantity * abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price <=0   
	then sls_sales / nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0  or sls_quantity <=0 or sls_price <=0
order by sls_sales, sls_quantity, sls_price;	

--Rules to apply to fix 0, negative or nulls
--1 if sales is negative, zero or null derive it using quantity and price,
--2 if price is zero or null calculate it using sales and quantity,
--3 if price is negative convert it to a positive value.

select * from silver.crm_sales_details;

-----------------------------------
--ERP table

insert into silver.erp_cust_az12(
cid,
bdate,
gen)
select
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end cid,
case when bdate> getdate() then null
	else bdate
end as bdate,
case when upper(trim(gen)) in ('F' ,'FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M' ,'MALE') then  'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12;


where case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end not in (select distinct cst_key from silver.crm_cust_info);

select distinct 
gen, 
case when upper(trim(gen)) in ('F' ,'FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M' ,'MALE') then  'Male'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12;



where cid like '%AW00011000%';

select * from silver.crm_cust_info;

select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

select distinct gen from silver.erp_cust_az12;

select * from silver.erp_cust_az12;

select top(3) * from bronze.erp_loc_a101;
select top(3) * from silver.erp_cust_az12;

select distinct cntry from DataWarehouse.silver.erp_loc_a101;

insert into silver.erp_loc_a101
(cid,
cntry)
select 
replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry)
end as cntry
from bronze.erp_loc_a101;

select
cid,
cntry
from bronze.erp_loc_a101
where cid not in (select cid from silver.crm_cust_info);

select * from silver.erp_loc_a101;
select cst_key from silver.crm_cust_info;

select distinct id from bronze.erp_px_cat_g1v2;
select distinct cat_id from silver.crm_prd_info;

select * from bronze.erp_px_cat_g1v2;
select * from silver.crm_prd_info;

select id from bronze.erp_px_cat_g1v2
where id not in (select prd_key from silver.crm_prd_info);

select distinct maintenance from bronze.erp_px_cat_g1v2
where maintenance != trim(maintenance);

insert into silver.erp_px_cat_g1v2
(id, 
cat,
subcat,
maintenance)
select id, cat, subcat, maintenance
from bronze.erp_px_cat_g1v2;

select * from silver.erp_px_cat_g1v2;
