/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--Customer dim table/view
-- First do the union

--Check if there are duplicates in cst_id
select cst_id, count(*) from 
	(select
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	from silver.crm_cust_info ci
	left join silver.erp_cust_az12 ca
	on			ci.cst_key = ca.cid
	left join silver.erp_loc_a101 la
	on			ci.cst_key = la.cid
	)t group by cst_id
	having count(*) > 1;

--Data integration for the same info coming from different tables
select distinct
	ci.cst_gndr,
	ca.gen,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
		 else coalesce(ca.gen, 'n/a')
	end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on			ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on			ci.cst_key = la.cid	
order by 1,2

select distinct gender from gold.dim_customers;

-----------------------------------------
--Product dim table/view
--Duplicate check
select prd_key, count(*) from
(select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_name,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null
)t group by prd_key
having count(*) > 1

create view gold.dim_products as
select
	row_number () over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,	
	pn.prd_key as product_number,
	pn.prd_name as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as product_cost,
	pn.prd_line as product_line,
	pn.prd_start_dt	as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null 

select * from gold.dim_products;

--Sales details fact table

--Foreign key integrity
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where c.customer_key is null;

select *
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null;
