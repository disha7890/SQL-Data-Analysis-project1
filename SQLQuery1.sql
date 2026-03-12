/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates
	a new database named 'DataWarehouseAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'DataWarehouseAnalytics' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\Dell\OneDrive\Documents\dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\Dell\OneDrive\Documents\dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\Dell\OneDrive\Documents\fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO
--over time-anaylsis(yearly)--
select 
year(order_date) as order_year,
Sum(sales_amount) as total_sales,
Count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date)

--over time-anaylsis--
select 
datetrunc(month, order_date) as order_date,
Sum(sales_amount) as total_sales,
Count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
order by datetrunc(month, order_date)

--total sales per month--
--and the running total of sales over time--
select 
order_date,
total_sales,
sum(total_sales) over(order by order_date) as running_total_sales,
avg(avg_price) over(order by order_date) as moving_average_price
from
(
select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales, 
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date)
) t


/* analyze ofyearly performance of products by comparing their sales to both avg
sales performance of the product and the previous year's sales */
with yearly_producr_sales as (
select 
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
group by
year(f.order_date),
p.product_name
) 

select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above Avg'
     when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below Avg'
	 else 'Avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) py_sales,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
     when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
	 else 'no change'
end py_change
from yearly_producr_sales
order by product_name, order_year

--proportional analysis--
--which category contributes the most to overall sales?
with category_sales as (
select
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select 
category,
total_sales,
sum(total_sales) over () overall_sales,
concat(round((cast (total_sales as float) / sum(total_sales) over ())*100,2), '%') as percentage_of_total_sales
from category_sales
order by total_sales desc

/*segment products into cast ranges and
count how many procusts fall into each segment*/
with product_segments as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'below 100'
     when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'above 1000'
end cost_range
from gold.dim_products)

select
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc

/* group customers into three segments based on their spending behaviour:
- VIP: customers with at least 12 months of history and spending more than 5000.
_ regular: customers with atleast 12 months of history but spending 5000 or less.
- new: Customers with a lifespan less than 12 months.
and total no. of customers by each group*/
with customer_spending as (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
DATEDIFF( month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key= c.customer_key
group by c.customer_key)

select
customer_segment,
count(customer_key) as total_customers
from(
  select
  customer_key,
  case when lifespan >= 12 and total_spending > 500 then 'VIP'
       when lifespan >= 12 and total_spending <= 500 then 'Regular'
	   else 'new'
  end customer_segment
  from customer_spending) t
group by customer_segment
order by total_customers desc

/*
------------CUSTOMER REPORT----------------
purpose:- this report consolidates key customer metrics and behaviors

highlights:
1. Gathers essential field such as names, ages, and transaction details.
2.Segments customers into categories (VIP, Regular, New) and age groups.
3. Aggregates customer-level metrics:
  - total orders
  - total sales
  - total quantity purchased
  - total products
  - lifespan (in months)
4.Calculates valuable KPIs:
  - recently (months since last order)
  - average order value
  - average monthly spend
  ---------------------------------------
  -----------------------
 1) BAse: retrieves core columns from tables*/
 
 create view gold.report_customers as
 with base_query as(
 select
 f.order_number,
 f.product_key,
 f.order_date,
 f.sales_amount,
 f.quantity,
 c.customer_key,
 c.customer_number,
 concat(c.first_name, ' ', c.last_name) as customer_name,
 DATEDIFF(year, c.birthdate, getdate()) age
 from gold.fact_sales f
 left join gold.dim_customers c
 on c.customer_key = f.customer_key
 where order_date is not null)

,customer_aggregation as (
select
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
datediff(month, min(order_date), max(order_date)) as lifespan
from base_query
group by 
 customer_key,
 customer_number,
 customer_name,
 age
)
select 
customer_key,
customer_number,
customer_name,
age,
case 
     when age < 20 then 'Under 20'
     when age between 20 and 29 then '20-29'
	 when age between 30 and 39 then '30-39'
	 when age between 40 and 49 then '40-49'
     else '50 and above'
  end as age_group,
 case 
      when lifespan >= 12 and total_sales > 5000 then 'VIP'
      when lifespan >= 12 and total_sales <= 5000 then 'Regular'
	  else 'new'
end as customer_segment,
last_order_date,
datediff(month, last_order_date, getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
--compute avg order value--
case when total_sales= 0 then 0
     else total_sales / total_orders
end as avg_order_value,

--compute average monthly spend--
case when lifespan =0 then total_sales
     else total_sales/ lifespan
end as ave_monthly_spend
from customer_aggregation

SELEct* from gold.report_customers




















