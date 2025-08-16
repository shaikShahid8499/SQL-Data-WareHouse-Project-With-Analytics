/*
============================================
Create Databse and Schemas
============================================
Script Purpose:
  This script creates a new database names 'DataWareHose' after checking if it already exists.
  If the database exists, it dropped and recreated. Additionally, the script sets up three schemas
  with in the database: Bronze, Silver, Gold.

Warning:
  Running this script drops the entire 'DataWarehouse' database if it exists.
  All data in the database permanently deleted. Proceed with caution
  and ensure you have proper backups before running this script.  
*/

USE master;
GO

--Drop and recreate the Database DataWareHouse'
IF EXISTS(	select 1 from sys.databases where name='DataWareHouse')
BEGIN
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO


--Create The DataBase 'DataWareHouse'
CREATE DATABASE DataWareHouse;
go


USE DataWareHouse;
go

--Create Schemas
CREATE SCHEMA Bronze;
GO

CREATE SCHEMA  Silver;
GO

CREATE SCHEMA  Gold;
GO
