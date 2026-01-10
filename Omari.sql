show tables;
select *
from customers_300;
select *
from inventory_300;
select *
from payments_300;
select *
from sales_300;
select *
from suppliers_300;
select *
from products_300;

-- starting on cleaning the data on the customers table
create table customers
select*
from customers_300;
select *
from customers;

-- checking for duplicates
select *,
ROW_NUMBER() over (PARTITION BY customer_id,customer_name,gender,age)as rownum
from customers;

with duplicate_cte as (
select *,
ROW_NUMBER() over (PARTITION BY customer_id,customer_name,gender,age)as rownum
from customers
)
select *
from duplicate_cte
where rownum>1;

-- we have lots of duplicates so we create a third table called customers2
CREATE TABLE `customers2` (
  `customer_id` text,
  `customer_name` text,
  `gender` text,
  `age` text,
  `rownum`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from customers2;

insert into customers2
select *,
ROW_NUMBER() over (PARTITION BY customer_id,customer_name,gender,age)as rownum
from customers;

select *
from customers2;

delete 
from customers2
where rownum>1;

select *
from customers2
where rownum=1;

select customer_id
from customers2
where  customer_id is null;
-- no nulls in the customer id column so we have to trim
select trim(customer_id),customer_id
from customers2;

update customers2
set customer_id=trim(customer_id);
select trim(customer_name),customer_name
from customers2;
update customers2
set customer_name=trim(customer_name);
select DISTINCT(gender),gender
from customers2
GROUP BY gender;
select gender,
replace(replace(gender, 'F', 'Female'),'M','Male')
from customers2;
update customers2
set gender=replace(replace(gender, 'F', 'Female'),'M','Male');
select DISTINCT(gender),gender
from customers2
GROUP BY gender;
update customers2
set gender=trim(gender);
select *
from customers2;
select gender,
replace(gender,'', 'Unknown')
from customers2;
select *
from customers2
where  gender like'';
select gender,
coalesce(nullif(gender,''),'Unknown') as new_gender
from customers2;
update customers2
set gender=coalesce(nullif(gender,''),'Unknown');
select *
from customers2;

select count(*)
from customers2
where age='';
select count(*)
from customers2;
select count(age) as missiing_ages,gender
from customers2
where age =''
group by gender;
-- we caanot drop the  blanks in the age as they customers will be very useful to our EDA so we will leve them as nulls
select age,
NULLIF(age,'') as replaced_age
from customers2;
update customers2
set age=NULLIF(age,'');
select COUNT(*)
from customers2
where age IS NULL;
alter TABLE customers2
modify column age int;
alter TABLE customers2
drop column rownum;
select *
from customers2;
alter TABLE customers2
modify column customer_id VARCHAR(10);
 ALTER table customers2
 add primary key (customer_id);
select *
from customers2;

-- MOVING TO THE INVENTORY TABLE
select *
from inventory_300;
select COUNT(*)
from inventory_300;

create table inventory
select *
from inventory_300;
select *
from inventory;
select *,
row_number() over(partition by inventory_id,product_id,supplier_id,stock_qty,expiry_date) as rownum
from inventory;

with duplicate_cte as (
select *,
row_number() over(partition by inventory_id,product_id,supplier_id,stock_qty,expiry_date) as rownum
from inventory
)
select *
from duplicate_cte
where rownum>1;CREATE TABLE `inventory2` (
  `inventory_id` int DEFAULT NULL,
  `product_id` text,
  `supplier_id` text,
  `stock_qty` text,
  `expiry_date` text,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into inventory2
select *,
row_number() over(partition by inventory_id,product_id,supplier_id,stock_qty,expiry_date) as rownum
from inventory;

select *
from inventory2;

delete 
from inventory2
where rownum>1;
alter table inventory2
drop COLUMN rownum;
select *
from inventory2 
where inventory_id is null;
-- inventory id can be our primary key
update inventory2
set product_id=trim(product_id);
update inventory2
set inventory_id=trim(inventory_id);
update inventory2
set supplier_id=trim(supplier_id);
update inventory2
set stock_qty=trim(stock_qty);
alter table inventory2
modify column product_id varchar(20);
alter table inventory2
modify column supplier_id varchar(20);
-- stock quantity should be in integer,but is impossible due to the missing values there by to change the number we need to first change the missing values to nulls
select stock_qty,
nullif(stock_qty,'')
from inventory2;

update inventory2
set stock_qty=nullif(stock_qty,'');
alter table inventory2
modify column stock_qty int;

select expiry_date,
str_to_date(expiry_date,'%Y-%m-%d')
from inventory2;
update inventory2
set expiry_date=str_to_date(expiry_date,'%Y-%m-%d');
select *
from inventory2;
-- moving on to the payments table
select *
from payments_300;
select count(*)
from payments_300;
create table payments 
select *
from payments_300;
select *
from payments;
select *,
row_number() over (partition by payment_id,sale_id,payment_method,amount_paid,payment_date) as rownum
from payments;

with duplicate_cte as (
select *,
row_number() over (partition by payment_id,sale_id,payment_method,amount_paid,payment_date) as rownum
from payments
)
select *
from duplicate_cte
where rownum>1;
CREATE TABLE `payments2` (
  `payment_id` int DEFAULT NULL,
  `sale_id` int DEFAULT NULL,
  `payment_method` text,
  `amount_paid` int DEFAULT NULL,
  `payment_date` text,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into payments2
select *,
row_number() over (partition by payment_id,sale_id,payment_method,amount_paid,payment_date) as rownum
from payments;
delete 
from payments2
where rownum>1;
update payments2
set payment_id=trim(payment_id);
update payments2
set sale_id=trim(sale_id);
update payments2
set payment_method=trim(payment_method);
update payments2
set amount_paid=trim(amount_paid);
update payments2
set payment_date=trim(payment_date);
alter table payments2
add primary key(payment_id);
alter table payments2
drop column rownum;
select DISTINCT(payment_method)
from payments;
select payment_method,
coalesce(nullif(payment_method,''),'Unknown') as New_
from payments2;
Update payments2
set payment_method=coalesce(nullif(payment_method,''),'Unknown');

select payment_date,
str_to_date(payment_date,'%Y-%m-%d')
from payments2;
update payments2
set payment_date=str_to_date(payment_date,'%Y-%m-%d');

-- Moviing on to the product TABLE
select *
from products_300;
create table products
select *
from products_300;
select *
from products;
select *,
row_number() over(partition by product_id,product_name,category,unit_price) as rownum
from products;
 with duplicate_cte as (
 select *,
row_number() over(partition by product_id,product_name,category,unit_price) as rownum
from products
)
select *
from duplicate_cte
where rownum>1;CREATE TABLE `products2` (
  `product_id` text,
  `product_name` text,
  `category` text,
  `unit_price` int DEFAULT NULL,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into products2
select *,
row_number() over(partition by product_id,product_name,category,unit_price) as rownum
from products;

delete 
from products2
where rownum>1;
select *
from products2;
update products2
set product_id=trim(product_id);
update products2
set product_name=trim(product_name);
update products2
set category=trim(category);
update products2
set unit_price=trim(unit_price);

select *
from products2
where product_id is null;
 alter table products2
 modify column product_id varchar(10);
alter table products2
add primary key(product_id);
select DISTINCT(product_name)
from products2;
select product_name,
coalesce(nullif(product_name,''),'Unknown') as fillers
from products2;
update products2 
set product_name=coalesce(nullif(product_name,''),'Unknown');
select *
from products2
where product_name='';
select DISTINCT(category)
from products2;
select *
from products2
where category='Antibiotic';

select avg(unit_price),category
from products2
GROUP BY category;
select category,
coalesce(nullif(category,''),'Unknown')
from products2;
update products2
set category=coalesce(nullif(category,''),'Unknown');
select *
from products2;
alter table products2
modify column product_id varchar(10);
select category,
replace(category,'Unknown','') as new_
from products2;

update products2
set category=replace(category,'Unknown','');

select *
from products2  as t1
join products2 as  t2
on t1.product_name=t2.product_name
and t1.unit_price=t2.unit_price
where  t1.category=''
and t2.category<>'';

update products2  as t1
join products2 as  t2
on t1.product_name=t2.product_name
and t1.unit_price=t2.unit_price
set t1.category=t2.category
where  t1.category=''
and t2.category<>'';

select product_name,
replace(product_name,'Unknown','') as new_
from products2;
update products2
set product_name=replace(product_name,'Unknown','');

select *
from products2 as t1
join products2 as t2
on t1.category=t2.category
and t1.unit_price=t2.unit_price
where t1.product_name=''
and t2.product_name<>'';

update products2 as t1
join products2 as t2
on t1.category=t2.category
and t1.unit_price=t2.unit_price
set t1.product_name=t2.product_name
where t1.product_name=''
and t2.product_name<>'';
select *
from products2;
 alter table products2
 drop column rownum;
-- moving on to the sales table
select *
from sales_300;
-- looking for the duplicates 
create table sales
select *
from sales_300;

select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum
from sales;
 with duplicate_cte as (
 select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum
from sales
)
select *
from duplicate_cte
where rownum>1;
CREATE TABLE `sales2` (
  `sale_id` int DEFAULT NULL,
  `customer_id` text,
  `product_id` text,
  `quantity` int DEFAULT NULL,
  `unit_price` text,
  `sale_date` text,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
insert into sales2
select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum
from sales;
delete 
from sales2
where rownum>1;
select *
from sales2;
update sales2
set sale_id=trim(sale_id);
update sales2
set customer_id=trim(customer_id);
update sales2
set product_id=trim(product_id);
update sales2
set quantity=trim(quantity);
update sales2
set unit_price=trim(unit_price);
update sales2
set sale_date=trim(sale_date);

alter table sales2
add primary key (sale_id);
select *
from sales2
where sale_id=10119;
select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum2
from sales2;

with duplicate_cte as (
select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum2
from sales2
)
select *
from duplicate_cte
where rownum2>1;
CREATE TABLE `sales3` (
  `sale_id` int DEFAULT NULL,
  `customer_id` text,
  `product_id` text,
  `quantity` int DEFAULT NULL,
  `unit_price` text,
  `sale_date` text,
  `rownum` int DEFAULT NULL,
  `rownum2`int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
 
 insert into sales3
 select *,
row_number() over(partition by sale_id,customer_id,product_id,quantity,unit_price,sale_date)as rownum2
from sales2;

select * 
from sales3;
delete 
from sales3
where rownum2>1;
select *
from sales3;
alter table sales3
add primary key (sale_id);
 select *,
 str_to_date(sale_date,'%Y-%m-%d')
 from sales3;
update sales3
set sale_date=str_to_date(sale_date,'%Y-%m-%d');
alter table sales3
drop column rownum;
alter table sales3
drop column rownum2;

-- moving on to the suppliers table
select *
from suppliers_300;
create table suppliers
select *
from suppliers_300;

select *,
row_number () over (partition by supplier_id,supplier_name,country) as rownum
from suppliers;

with duplicate_cte as (
select *,
row_number () over (partition by supplier_id,supplier_name,country) as rownum
from suppliers
)
select *
from duplicate_cte
WHERE rownum>1;

CREATE TABLE `suppliers2` (
  `supplier_id` text,
  `supplier_name` text,
  `country` text,
  `rownum` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into suppliers2
select *,
row_number () over (partition by supplier_id,supplier_name,country) as rownum
from suppliers;
delete 
from suppliers2
where rownum>1;
update suppliers2
set supplier_id=trim(supplier_id);
update suppliers2
set supplier_name=trim(supplier_name);
update suppliers2
set country=trim(country);
select count(*)
from suppliers2;
select *
from suppliers2;
 alter table suppliers2
 modify column supplier_id varchar(10);

alter table suppliers2
add primary key (supplier_id);
select DISTINCT(supplier_name)
from suppliers2;
select DISTINCT(country)
from suppliers2;
select *
from suppliers2 as t1
join suppliers as t2
on t1.country=t2.country
where t1.supplier_name=''
and t2.supplier_name<>'';
 
 update suppliers2 as t1
join suppliers2 as t2
on t1.country=t2.country
set t1.supplier_name=t2.supplier_name
where t1.supplier_name=''
and t2.supplier_name<>'';

select *
from suppliers2 as t1
join suppliers2 as t2
on t1.supplier_name=t2.supplier_name
where t1.country=''
and t2.country <>'';

update suppliers2 as t1
join suppliers2 as t2
on t1.supplier_name=t2.supplier_name
set t1.country=t2.country
where t1.country=''
and t2.country <>'';
alter table suppliers2
drop column rownum;


-- after cleaning of the table we now can create relationships between the tables and data

select *
from customers2;
select *
from inventory2;
select *
from payments2;
select *
from products2;
select *
from sales3;
select *
from suppliers2;

alter table inventory2
add foreign key (supplier_id)
references suppliers2(supplier_id);

alter table sales3
add foreign key (customer_id)
references customers2(customer_id);
alter table inventory2
add primary key (inventory_id);


 alter table sales3
 MODIFY COLUMN customer_id VARCHAR(10);
 
 
 select *
from customers2;
select *
from inventory2;
select *
from payments2;
select *
from products2;
select *
from sales3;
select *
from suppliers2;
 -- getting into our eda
 select A.inventory_id ,F.supplier_id,D.product_id,F.supplier_name,F.country,D.product_name
 from inventory2 as A
 JOIN suppliers2 AS F ON A.supplier_id=F.supplier_id
 JOIN products2 AS D ON A.product_id=D.product_id;
 
 SELECT C.payment_id,E.sale_id,A.customer_id,D.product_id,A.customer_name,D.product_name,D.unit_price,E.quantity,(D.unit_price*E.quantity) as amount_payable,C.amount_paid,(C.amount_paid-D.unit_price*E.quantity) AS status_of_payments
from sales3 as E
LEFT JOIN payments2 AS C on E.sale_id=C.sale_id
LEFT JOIN customers2 AS A on E.customer_id=A.customer_id
LEFT JOIN products2 AS D on E.product_id=D.product_id; -- this would need further investigation as there are paid  amounts without any products recording. so the question would be why?
 SELECT C.payment_id,E.sale_id,A.customer_id,D.product_id,A.customer_name,D.product_name,D.unit_price,E.quantity,(D.unit_price*E.quantity) as amount_payable,C.amount_paid,(C.amount_paid-D.unit_price*E.quantity) AS status_of_payments
from sales3 as E
 RIGHT JOIN payments2 AS C on E.sale_id=C.sale_id
 LEFT JOIN customers2 AS A on E.customer_id=A.customer_id
JOIN products2 AS D on E.product_id=D.product_id; -- this is the table off how much each brought INSERT

-- we want to see what products are in the sales table but not in the products table

 SELECT E.sale_id,D.product_id,D.product_name,D.unit_price
from sales3 as E
LEFT JOIN products2 AS D on E.product_id=D.product_id
WHERE E.product_id NOT IN(SELECT product_id
FROM products2);-- these are the products that gave been sold but are not in the products table. Means they need to be seen


SELECT E.sale_id,D.product_id,D.product_name,D.unit_price
from sales3 as E
LEFT JOIN products2 AS D on E.product_id=D.product_id
WHERE E.product_id IN(SELECT product_id
FROM products2); -- products that were sold and we actually have in our products

-- which of our products is briging a lot of money?
select *
from customers2;
select *
from inventory2;
select *
from payments2;
select *
from products2;
select *
from sales3;
select *
from suppliers2;
 select E.sale_id,D.product_id,D.product_name,D.category,C.payment_method,C.amount_paid
 from sales3 as E
 JOIN products2 AS D ON E.product_id=D.product_id
join payments2 as C on E.sale_id=C.sale_id;
 select count(E.sale_id),D.category,sum(C.amount_paid)as total_per_category
 from sales3 as E
 JOIN products2 AS D ON E.product_id=D.product_id
join payments2 as C on E.sale_id=C.sale_id
group by D.category;
 select COUNT(E.sale_id) as counted_id,C.payment_method,sum(C.amount_paid) AS total_per_paymentmethod
 from sales3 as E
 JOIN products2 AS D ON E.product_id=D.product_id
join payments2 as C on E.sale_id=C.sale_id
GROUP BY C.payment_method;













