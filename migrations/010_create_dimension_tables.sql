/*
Create core dimension tables that support both direct DB and API-driven ETL
Author: Repo Cleanup Automation
Date: November 18, 2025
*/

USE MarryBrown_DW;
GO

PRINT 'Creating dimension tables...';
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
BEGIN
    CREATE TABLE dbo.dim_date (
        DateKey INT PRIMARY KEY,
        FullDate DATE NOT NULL,
        DayOfWeek INT,
        DayName VARCHAR(10),
        DayOfMonth INT,
        DayOfYear INT,
        WeekOfYear INT,
        MonthName VARCHAR(10),
        MonthOfYear INT,
        Quarter INT,
        Year INT,
        IsWeekend BIT,
        IsHoliday BIT DEFAULT 0
    );
    PRINT '  [OK] Created dim_date';
END
ELSE PRINT '  - dim_date already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_time')
BEGIN
    CREATE TABLE dbo.dim_time (
        TimeKey INT PRIMARY KEY,
        FullTime TIME NOT NULL,
        Hour INT,
        Minute INT,
        Second INT,
        HourName VARCHAR(10),
        PeriodOfDay VARCHAR(20)
    );
    PRINT '  [OK] Created dim_time';
END
ELSE PRINT '  - dim_time already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_locations')
BEGIN
    CREATE TABLE dbo.dim_locations (
        LocationKey INT IDENTITY(1,1) PRIMARY KEY,
        LocationGUID NVARCHAR(50) NOT NULL,
        LocationName NVARCHAR(200),
        City NVARCHAR(100),
        State NVARCHAR(100),
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_locations';
END
ELSE PRINT '  - dim_locations already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_products')
BEGIN
    CREATE TABLE dbo.dim_products (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        SourceProductID NVARCHAR(50) NOT NULL,
        ProductCode NVARCHAR(50),
        ProductName NVARCHAR(200),
        Category NVARCHAR(100),
        ProductType NVARCHAR(50),
        Brand NVARCHAR(100),
        CurrentSalePrice DECIMAL(18,2),
        IsPackage BIT DEFAULT 0,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_products';
END
ELSE PRINT '  - dim_products already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customers')
BEGIN
    CREATE TABLE dbo.dim_customers (
        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerGUID NVARCHAR(50) NOT NULL,
        CustomerCode NVARCHAR(500),
        FullName NVARCHAR(500),
        FirstName NVARCHAR(500),
        LastName NVARCHAR(500),
        MobileNumber NVARCHAR(50),
        Email NVARCHAR(200),
        CustomerGroup NVARCHAR(100),
        CurrentLoyaltyPoints DECIMAL(18,2),
        RegistrationDate DATE,
        DateOfBirth DATE,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_customers';
END
ELSE PRINT '  - dim_customers already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_staff')
BEGIN
    CREATE TABLE dbo.dim_staff (
        StaffKey INT IDENTITY(1,1) PRIMARY KEY,
        StaffUsername NVARCHAR(200) NOT NULL,
        StaffFullName NVARCHAR(200),
        StaffType NVARCHAR(100)
    );
    PRINT '  [OK] Created dim_staff';
END
ELSE PRINT '  - dim_staff already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_payment_types')
BEGIN
    CREATE TABLE dbo.dim_payment_types (
        PaymentTypeKey INT IDENTITY(1,1) PRIMARY KEY,
        PaymentMethodName NVARCHAR(200) NOT NULL,
        PaymentCategory NVARCHAR(50)
    );
    PRINT '  [OK] Created dim_payment_types';
END
ELSE PRINT '  - dim_payment_types already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_promotions')
BEGIN
    CREATE TABLE dbo.dim_promotions (
        PromotionKey INT IDENTITY(1,1) PRIMARY KEY,
        SourcePromotionID NVARCHAR(50) NOT NULL,
        PromotionName NVARCHAR(200),
        PromotionDescription NVARCHAR(500),
        PromotionCode NVARCHAR(200),
        PromotionType NVARCHAR(50),
        StartDate DATE,
        EndDate DATE,
        IsActive BIT DEFAULT 1
    );
    PRINT '  [OK] Created dim_promotions';
END
ELSE PRINT '  - dim_promotions already exists';

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_terminals')
BEGIN
    CREATE TABLE dbo.dim_terminals (
        TerminalKey INT IDENTITY(1,1) PRIMARY KEY,
        TerminalID NVARCHAR(50) NOT NULL,
        LocationKey INT,
        TerminalName NVARCHAR(100),
        IsActive BIT DEFAULT 1,
        FOREIGN KEY (LocationKey) REFERENCES dbo.dim_locations(LocationKey)
    );
    PRINT '  [OK] Created dim_terminals';
END
ELSE PRINT '  - dim_terminals already exists';

PRINT 'Dimension table creation complete.';
GO

