--checking for uniqueness of customer_key in Gold.dim_customers
select
	customer_key,
	count(*) as duplicate_count
from Gold.dim_customers
group by customer_key
having count(*)>1;


--checking for uniqueness of product_key in Gold.dim_products
select
	product_key,
	count(*) as duplicate_count
from Gold.dim_products
group by product_key
having count(*)>1;


--foreign key integrity
select * from
Gold.fact_sales f
left join Gold.dim_products p
ON f.product_key=p.product_key
left join Gold.dim_customers c
ON f.customer_key=c.customer_key
where p.product_key IS NULL or c.customer_key IS NULL
