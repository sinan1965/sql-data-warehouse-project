--Change over time year
select
year(order_date) as order_year,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
count(distinct product_key) as total_products,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);

--Change over time month
select
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by year(order_date), month(order_date)
order by year(order_date), month(order_date);


--Change over time year&month
select
datetrunc(month, order_date) as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
count(distinct product_key) as total_products,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date);

------------------------------------------------------
--Cumulative analysis
--calculate total sales per month

select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
order by datetrunc(month,order_date);


--the running total of sales over time

select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales
from
(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
) t

--calculate the moving average
select
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
select 
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
) t

--Performance analysis

--analyse the yearly performance of products
with yearly_product_sales as(
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
group by year(f.order_date), p.product_name
)
select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name)>0 then 'Above average'
	 when current_sales - avg(current_sales) over (partition by product_name)<0 then 'Below average'
	 else 'Average'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) <0 then 'Decreasing'
	 when current_sales - lag(current_sales) over (partition by product_name order by order_year) >0 then 'Increasing'
	 else 'No change'
end py_change
from yearly_product_sales
order by product_name, order_year;

--part-to-whole analysis
--which categories contribute the most to overall sales
with category_sales as(
select 
p.category,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by p.category)
select
category,
total_sales,
sum(total_sales) over() as overall_sales,
concat('%',round((cast (total_sales as float)/ sum(total_sales) over()) *100,2))as sales_percent
from category_sales
order by sales_percent desc;

--Data segmentation
with product_segments as(
select 
product_key,
product_name,
product_cost,
case when product_cost < 100 then 'Below 100'
	 when product_cost between 100 and 500 then '100-500'
	 when product_cost between 500 and 1000 then '500-1000'
	 else 'Above 1000'
end as cost_range
from gold.dim_products)

select 
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;

---------
with customer_profiles as(
select 
c.customer_key,
sum(f.sales_amount) as total_spend,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
datediff(month,min(f.order_date), max(f.order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key)

select
customer_key,
total_spend,
lifespan,
case when lifespan >= 12 and total_spend > 5000 then 'VIP'
	 when lifespan >= 12 and total_spend <= 5000 then 'Regular'
	 else 'New'
end as customer_segments
from customer_profiles


