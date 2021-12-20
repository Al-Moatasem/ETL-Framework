USE master;
GO

if not exists (select 1 from sys.databases where name = N'UDEMY')
CREATE DATABASE UDEMY
GO


use UDEMY;
go

if not exists (select 1 from sys.schemas where name = N'stg')
EXEC ('CREATE SCHEMA stg')
GO

if not exists (select 1 from sys.schemas where name = N'dwh')
EXEC ('CREATE SCHEMA dwh')