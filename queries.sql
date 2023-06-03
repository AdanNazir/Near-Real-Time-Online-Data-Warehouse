use starschema;

#Q1

select s.store_name as "Store_Name",sum(f.total_sale) as "Total_Sale",t.Month as "Month",t.yearly as "Year" From store s join metro_sales 
f join t_date t  on 
(s.store_id=f.store_id and t.time_id=f.time_id and t.month='SEPTEMBER' and t.yearly='2017')
 group by Store_Name,Total_Sale,Month,Year
 order by  Total_Sale desc  limit 3;

#Q2

select s.supplier_name as "Supplier_Name",f.total_sale as "Highest_Sale" from supplier s join metro_sales f join t_date t on 
 (s.supplier_id=f.supplier_id and t.time_id=f.time_id and (t.week='SUNDAY' or t.week='SATURDAY'))GROUP BY Supplier_Name,
 total_sale ORDER BY  total_sale Desc limit 10;


#Q3

select s.supplier_name as "Supplier_Name",p.product_name as "Product",sum(f.total_sale) as "Total_Sale",t.quarterly as "Quarter",
 t.month as
 "Month" from supplier s join metro_sales f join product p join t_date t 
 on (s.supplier_id=f.supplier_id and p.product_id=f.product_id and t.time_id=f.time_id)
 group by Supplier_Name,Product,Total_Sale,quarter,month;

#Q4

select s.store_name as "Store_Name",p.product_name as "Product",sum(f.total_sale) as "Total_Sale" from store s join metro_sales 
 f join product p
 on (s.store_id=f.store_id and p.product_id=f.product_id)
 group by Store_Name,Product,Total_Sale
 order by Store_Name,Product;

#Q5

select s.store_name  , t.quarterly  , sum(f.total_sale) as "Total Sale"
 from store s join  metro_sales f join t_date t  on (s.STORE_ID = f.STORE_ID and f.time_Id = t.time_Id)
 group by store_name,quarterly
order by store_name,quarterly;

#Q6

select p.product_name as "Product_Name",f.total_sale as "Highest_Sale" from product p join metro_sales f join t_date t  on 
 (p.product_id=f.product_id and t.time_id=f.time_id and (t.day='6' or t.day='7' or t.day='13' or t.day='14' or t.day='20' or t.day
 ='21' or t.day='27' or t.day='28') )group by Product_Name,Highest_Sale 
 order by Highest_Sale desc limit 5;

#Q7

select s.store_name,c.supplier_name,p.product_name,sum(f.total_sale) as "Total Sale" from store s join product p
 join supplier c join 
 metro_sales f on
 ( s.store_id=f.store_id and p.product_id=f.product_id and c.supplier_id=f.supplier_id) 
 GROUP BY store_name,supplier_name,product_name,total_sale with rollup;

#Q9

select product_id,product_name from product where product_name='Tomatoes';


