/*
 Create Database and Schemas
 
 Script Purpose: This script intends to create a new database called 'DataWarehouse' and three schemas within the database.
 Warning:  If the database already exists, it is dropped and re-created
 */


use master;

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
begin
	alter DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	drop database DataWarehouse;
end;
go

create database DataWarehouse;

use DataWarehouse;
GO

create schema bronze;
go

create schema silver;
go

create schema gold;
go
