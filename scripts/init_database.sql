/*
-----------------------------------------------
Create Database and Schemas
-----------------------------------------------
Script Purpose:
  This script creates a database named 'DataWarehouse' after checking if it already exists. 
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
  within the database: 'bronze', 'silver' and 'gold'.

Warning:
  Running this script will drop entire 'DataWarehouse' database if it exists.
  All data in the database will be permamently deleted.Proceed with caution
  and ensure you have proper bacups before running this script.
*/
  
/*
Drop and recreate the 'DataWarehouse' database
--if exists (select 1 from sys.databases where name = 'DataWarehouse')
--begin
  --alter database DataWarehouse set single_user with rollback immediate;
  --drop database DataWarehouse;
--end;
--go
*/

--Create the 'DataWarehouse' database
use master;
go
  
create database DataWarehouse;
go

use DataWarehouse;
go

--Create Schemas  
create schema bronze;
go

create schema silver;
go

create schema gold;
