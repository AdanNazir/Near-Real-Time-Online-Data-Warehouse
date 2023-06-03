drop schema if exists `starschema` ;
commit;
Create schema starschema;
use starschema;
drop table if exists metro_sales;
drop table if exists product;
drop table if exists customer;
drop table if exists store;
drop table if exists supplier;
drop table if exists t_date;



CREATE TABLE `Product` (
  `PRODUCT_ID` VARCHAR(6) NOT NULL , 
  `PRODUCT_Name` VARCHAR(255) NOT NULL,
  primary key(`PRODUCT_ID`));
CREATE TABLE `CUSTOMER` (
  `CUSTOMER_ID` VARCHAR(6) NOT NULL, 
  `CUSTOMER_NAME` VARCHAR(255) NOT NULL,
  primary key(`CUSTOMER_ID`));
CREATE TABLE `STORE` (
  `STORE_ID` VARCHAR(6) NOT NULL, 
  `STORE_Name` VARCHAR(255) NOT NULL,
  primary key(`STORE_ID`));
CREATE TABLE `Supplier` (
  `SUPPLIER_ID` VARCHAR(6) NOT NULL, 
  `SUPPLIER_Name` VARCHAR(255) NOT NULL,
  primary key(`SUPPLIER_ID`));
CREATE TABLE `T_date` (
  `Time_Id` VARCHAR(25) NOT NULL,
  `Day` int NOT NULL,
  `Week` VARCHAR(10) NOT NULL,
  `Month` VARCHAR(10) NOT NULL,
  `Quarterly` int NOT NULL,
  Yearly int NOT NULL,
  primary key(`Time_Id`));
  
  CREATE TABLE `Metro_sales` (
  Transaction_ID Integer NOT NULL,
  primary key(`Transaction_ID`),
  PRODUCT_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (PRODUCT_ID) REFERENCES Product(PRODUCT_ID),
  CUSTOMER_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER(CUSTOMER_ID),
  STORE_ID VARCHAR(6) NOT NULL,
  FOREIGN KEY (STORE_ID) REFERENCES STORE(STORE_ID),
  SUPPLIER_ID VARCHAR(6) NOT NULL,
   FOREIGN KEY (SUPPLIER_ID) REFERENCES SUPPLIER(SUPPLIER_ID),
  Time_Id VARCHAR(25) NOT NULL,
  FOREIGN KEY (Time_Id) REFERENCES T_date(Time_Id),
  Total_Sale Float NOT NULL
  );
  
  use db;
DROP TABLE if exists Transactions_data;
create table transactions_data as 
select t.transaction_id,t.product_id, t.customer_id,customers.customer_name,t.store_id,
t.store_name,t.time_id,t.t_date,t.quantity from customers join transactions t on (customers.customer_id
=t.customer_id);

commit;
  






