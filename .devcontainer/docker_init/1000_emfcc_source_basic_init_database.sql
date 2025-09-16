IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]='emfcc_source_basic')
BEGIN
    EXEC('CREATE DATABASE [emfcc_source_basic]')
    PRINT '[emfcc_source_basic] database created'
END
GO
USE [emfcc_source_basic]
GO
SET NOCOUNT ON
GO


-- ==============================================================================
-- Create CardType table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='CardType')
EXEC ('CREATE TABLE [dbo].[CardType]
(
    [CardTypeID]        INT                         PRIMARY KEY,
    [TypeName]          NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]       NVARCHAR(255)       NULL,
    [CreatedOn]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]        DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[CardType]) = 0
INSERT INTO [dbo].[CardType] ([CardTypeID], [TypeName], [Description], [CreatedOn])
VALUES
    (1, 'Visa',             'Visa Credit or Debit Card',        '2025-06-01T08:00:00'),
    (2, 'MasterCard',       'MasterCard Credit or Debit Card',  '2025-06-01T08:00:00'),
    (3, 'American Express', 'American Express Credit Card',     '2025-06-01T08:00:00'),
    (4, 'Discover',         'Discover Credit Card',             '2025-06-01T08:00:00'),
    (5, 'Debit',            'Generic',                          '2025-06-01T08:00:00')
GO
CREATE OR ALTER TRIGGER [dbo].[trg_CardType_Update]
ON [dbo].[CardType]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[CardType]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[CardType].[CardTypeID] = inserted.[CardTypeID];
END
GO
PRINT '[dbo].[CardType] done'
GO








-- ==============================================================================
-- Create TransactionStatus table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='TransactionStatus')
EXEC ('CREATE TABLE [dbo].[TransactionStatus]
(
    [TransactionStatusID]   INT                         PRIMARY KEY,
    [StatusName]            NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedOn]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]            DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[TransactionStatus]) = 0
INSERT INTO [dbo].[TransactionStatus] ([TransactionStatusID], [StatusName], [Description], [CreatedOn])
VALUES
    (1, 'Pending',  'Transaction is initiated but not yet approved', '2025-06-01T08:00:00'),
    (2, 'Approved', 'Transaction was approved successfully', '2025-06-01T08:00:00'),
    (3, 'Declined', 'Transaction was declined by issuer or processor', '2025-06-01T08:00:00'),
    (4, 'Settled',  'Transaction has been cleared and funds settled', '2025-06-01T08:00:00'),
    (5, 'Reversed', 'Transaction was reversed or refunded', '2025-06-01T08:00:00');
GO
CREATE OR ALTER TRIGGER [dbo].[trg_TransactionStatus_Update]
ON [dbo].[TransactionStatus]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[TransactionStatus]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[TransactionStatus].[TransactionStatusID] = inserted.[TransactionStatusID];
END
GO
PRINT '[dbo].[TransactionStatus] done'
GO







-- ==============================================================================
-- Create TransactionType table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='TransactionType')
EXEC ('CREATE TABLE [dbo].[TransactionType]
(
    [TransactionTypeID]     INT                         PRIMARY KEY,
    [TypeName]              NVARCHAR(50)    NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedOn]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]            DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[TransactionType]) = 0
INSERT INTO [dbo].[TransactionType] ([TransactionTypeID], [TypeName], [Description], [CreatedOn])
VALUES
    (1, 'Purchase', 'Standard purchase transaction', '2025-06-01T08:00:00'),
    (2, 'Refund', 'Refund issued to the cardholder', '2025-06-01T08:00:00'),
    (3, 'Cash Advance', 'Cash withdrawal from ATM or teller', '2025-06-01T08:00:00'),
    (4, 'Fee', 'Bank or service fee applied', '2025-06-01T08:00:00'),
    (5, 'Interest', 'Interest charges on balance', '2025-06-01T08:00:00');
GO
CREATE OR ALTER TRIGGER [dbo].[trg_TransactionType_Update]
ON [dbo].[TransactionType]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[TransactionType]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[TransactionType].[TransactionTypeID] = inserted.[TransactionTypeID];
END
GO
PRINT '[dbo].[TransactionType] done'
GO






-- ==============================================================================
-- Create Currency table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Currency')
EXEC ('CREATE TABLE [dbo].[Currency]
(
    [CurrencyID]            INT                         PRIMARY KEY,
    [CurrencyCode]          CHAR(3)         NOT NULL    UNIQUE, -- ISO 4217 code
    [CurrencyName]          NVARCHAR(50)    NOT NULL,
    [Symbol]                NVARCHAR(5)         NULL,
    [CreatedOn]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]            DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Currency]) = 0
INSERT INTO [dbo].[Currency] ([CurrencyID], [CurrencyCode], [CurrencyName], [Symbol], [CreatedOn])
VALUES
    (1, 'EUR', 'Euro', '€', '2025-06-01T08:00:00'),
    (2, 'USD', 'US Dollar', '$', '2025-06-01T08:00:00');
GO
CREATE OR ALTER TRIGGER [dbo].[trg_Currency_Update]
ON [dbo].[Currency]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[Currency]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[Currency].[CurrencyID] = inserted.[CurrencyID];
END
GO
PRINT '[dbo].[Currency] done'
GO









-- ==============================================================================
-- Create MerchantCategory table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='MerchantCategory')
EXEC ('CREATE TABLE [dbo].[MerchantCategory]
(
    [MerchantCategoryID]    INT                         PRIMARY KEY,
    [CategoryName]          NVARCHAR(100)   NOT NULL    UNIQUE,
    [Description]           NVARCHAR(255)       NULL,
    [CreatedOn]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]            DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[MerchantCategory]) = 0
INSERT INTO [dbo].[MerchantCategory] ([MerchantCategoryID], [CategoryName], [Description], [CreatedOn])
VALUES
    (1, 'Retail', 'Stores selling consumer goods', '2025-06-01T08:00:00'),
    (2, 'Travel', 'Airlines, hotels, and travel services', '2025-06-01T08:00:00'),
    (3, 'Food & Beverage', 'Restaurants, cafes, bars', '2025-06-01T08:00:00'),
    (4, 'Entertainment', 'Movies, concerts, events', '2025-06-01T08:00:00'),
    (5, 'Utilities', 'Electricity, water, internet services', '2025-06-01T08:00:00'),
    (6, 'Health & Wellness', 'Pharmacies, gyms, clinics', '2025-06-01T08:00:00');
GO
CREATE OR ALTER TRIGGER [dbo].[trg_MerchantCategory_Update]
ON [dbo].[MerchantCategory]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[MerchantCategory]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[MerchantCategory].[MerchantCategoryID] = inserted.[MerchantCategoryID];
END
GO
PRINT '[dbo].[MerchantCategory] done'
GO









-- ==============================================================================
-- Create Merchant table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Merchant')
EXEC ('CREATE TABLE [dbo].[Merchant]
(
    [MerchantID]            INT                         PRIMARY KEY,
    [MerchantName]          NVARCHAR(150)   NOT NULL,
    [MerchantCategoryID]    INT             NOT NULL,
    [Address]               NVARCHAR(255)       NULL,
    [City]                  NVARCHAR(100)       NULL,
    [Country]               NVARCHAR(100)       NULL,
    [CreatedOn]             DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]            DATETIME2           NULL,
    CONSTRAINT [FK_Merchant_Category] FOREIGN KEY ([MerchantCategoryID]) REFERENCES [dbo].[MerchantCategory]([MerchantCategoryID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Merchant]) = 0
INSERT INTO [dbo].[Merchant] ([MerchantID], [MerchantName], [MerchantCategoryID], [Address], [City], [Country], [CreatedOn])
VALUES
(1, 'Walmart', 1, '702 SW 8th St', 'Bentonville', 'USA', '2025-06-01T08:00:00'),
(2, 'Target', 1, '1000 Nicollet Mall', 'Minneapolis', 'USA', '2025-06-01T08:00:00'),
(3, 'Best Buy', 1, '7601 Penn Ave S', 'Richfield', 'USA', '2025-06-01T08:00:00'),
(4, 'IKEA', 1, '420 Alan Wood Rd', 'Conshohocken', 'USA', '2025-06-01T08:00:00'),
(5, 'H&M', 1, 'Västra Hamngatan 3', 'Stockholm', 'Sweden', '2025-06-01T08:00:00'),
(6, 'Hilton Hotels', 2, '7930 Jones Branch Dr', 'McLean', 'USA', '2025-06-01T08:00:00'),
(7, 'Marriott', 2, '10400 Fernwood Rd', 'Bethesda', 'USA', '2025-06-01T08:00:00'),
(8, 'Expedia', 2, '333 108th Ave NE', 'Bellevue', 'USA', '2025-06-01T08:00:00'),
(9, 'Delta Airlines', 2, '1030 Delta Blvd', 'Atlanta', 'USA', '2025-06-01T08:00:00'),
(10, 'Airbnb', 2, '888 Brannan St', 'San Francisco', 'USA', '2025-06-01T08:00:00'),
(11, 'Starbucks', 3, '2401 Utah Ave S', 'Seattle', 'USA', '2025-06-01T08:00:00'),
(12, 'McDonald''s', 3, '110 N Carpenter St', 'Chicago', 'USA', '2025-06-01T08:00:00'),
(13, 'Burger King', 3, '5505 Blue Lagoon Dr', 'Miami', 'USA', '2025-06-01T08:00:00'),
(14, 'Pizza Hut', 3, '7100 Corporate Dr', 'Plano', 'USA', '2025-06-01T08:00:00'),
(15, 'Subway', 3, '325 Sub Way', 'Milford', 'USA', '2025-06-01T08:00:00'),
(16, 'AMC Theatres', 4, '11500 NW 105th St', 'Kansas City', 'USA', '2025-06-01T08:00:00'),
(17, 'Cinemark', 4, '3900 N Stemmons Fwy', 'Dallas', 'USA', '2025-06-01T08:00:00'),
(18, 'Live Nation', 4, '9348 Civic Center Dr', 'Beverly Hills', 'USA', '2025-06-01T08:00:00'),
(19, 'Ticketmaster', 4, '800 Connecticut Ave NW', 'Washington', 'USA', '2025-06-01T08:00:00'),
(20, 'Spotify', 4, '4 World Trade Center', 'New York', 'USA', '2025-06-01T08:00:00'),
(21, 'Comcast', 5, '1701 JFK Blvd', 'Philadelphia', 'USA', '2025-06-01T08:00:00'),
(22, 'AT&T', 5, '208 S Akard St', 'Dallas', 'USA', '2025-06-01T08:00:00'),
(23, 'Verizon', 5, '1095 Avenue of the Americas', 'New York', 'USA', '2025-06-01T08:00:00'),
(24, 'E.ON', 5, 'Brüsseler Str. 57', 'Essen', 'Germany', '2025-06-01T08:00:00'),
(25, 'Pfizer', 6, '235 E 42nd St', 'New York', 'USA', '2025-06-01T08:00:00'),
(26, 'CVS Pharmacy', 6, 'One CVS Drive', 'Woonsocket', 'USA', '2025-06-01T08:00:00'),
(27, 'Walgreens', 6, '200 Wilmot Rd', 'Deerfield', 'USA', '2025-06-01T08:00:00'),
(28, 'Planet Fitness', 6, '4 Liberty Ln W', 'Hampton', 'USA', '2025-06-01T08:00:00'),
(29, 'LA Fitness', 6, '2600 Michelson Dr', 'Irvine', 'USA', '2025-06-01T08:00:00'),
(30, 'Rite Aid', 6, '30 Hunter Ln', 'Camp Hill', 'USA', '2025-06-01T08:00:00')
GO
CREATE OR ALTER TRIGGER [dbo].[trg_Merchant_Update]
ON [dbo].[Merchant]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[Merchant]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[Merchant].[MerchantID] = inserted.[MerchantID];
END
GO
PRINT '[dbo].[Merchant] done'
GO








-- ==============================================================================
-- Create Customer table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Customer')
EXEC ('CREATE TABLE [dbo].[Customer]
(
    [CustomerID]    INT                         PRIMARY KEY,
    [FirstName]     NVARCHAR(100)   NOT NULL,
    [LastName]      NVARCHAR(100)   NOT NULL,
    [Email]         NVARCHAR(150)       NULL,
    [PhoneNumber]   NVARCHAR(20)        NULL,
    [DateOfBirth]   DATE                NULL,
    [CreatedOn]     DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]    DATETIME2           NULL
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Customer]) = 0
INSERT INTO [dbo].[Customer] ([CustomerID], [FirstName], [LastName], [Email], [PhoneNumber], [DateOfBirth], [CreatedOn])
VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '+1-555-1010', '1985-03-15', '2025-06-01T08:00:00'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '+1-555-2020', '1990-07-22', '2025-06-01T08:00:00'),
(3, 'Michael', 'Johnson', 'michael.johnson@example.com', '+1-555-3030', '1978-12-05', '2025-06-01T08:00:00'),
(4, 'Emily', 'Brown', 'emily.brown@example.com', '+1-555-4040', '1995-09-10', '2025-06-01T08:00:00'),
(5, 'David', 'Wilson', 'david.wilson@example.com', '+1-555-5050', '1982-05-30', '2025-06-01T08:00:00')
GO
CREATE OR ALTER TRIGGER [dbo].[trg_Customer_Update]
ON [dbo].[Customer]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[Customer]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[Customer].[CustomerID] = inserted.[CustomerID];
END
GO
PRINT '[dbo].[Customer] done'
GO









-- ==============================================================================
-- Create CardAccount table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='CardAccount')
EXEC ('CREATE TABLE [dbo].[CardAccount]
(
    [CardAccountID]     INT                         PRIMARY KEY,
    [CustomerID]        INT             NOT NULL,
    [AccountNumber]     NVARCHAR(20)    NOT NULL    UNIQUE,
    [Balance]           DECIMAL(18,2)   NOT NULL    DEFAULT 0,
    [CreditLimit]       DECIMAL(18,2)       NULL,
    [CurrencyID]        INT             NOT NULL,
    [CreatedOn]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]        DATETIME2           NULL,
    CONSTRAINT [FK_CardAccount_Customer] FOREIGN KEY ([CustomerID]) REFERENCES [dbo].[Customer]([CustomerID]),
    CONSTRAINT [FK_CardAccount_Currency] FOREIGN KEY ([CurrencyID]) REFERENCES [dbo].[Currency]([CurrencyID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[CardAccount]) = 0
INSERT INTO [dbo].[CardAccount] ([CardAccountID], [CustomerID], [AccountNumber], [Balance], [CreditLimit], [CurrencyID], [CreatedOn])
VALUES
(110, 1, 'ACE10110', 0.00,  5000.00, 1, '2025-06-01T08:00:00'), -- John Doe, EUR
(120, 2, 'ACU10120', 0.00,  7000.00, 2, '2025-06-01T08:00:00'), -- Jane Smith, USD
(130, 3, 'ACU10130', 0.00, 10000.00, 2, '2025-06-01T08:00:00'), -- Michael Johnson, USD
(140, 4, 'ACE10140', 0.00,  3000.00, 1, '2025-06-01T08:00:00'), -- Emily Brown, EUR
(150, 5, 'ACU10150', 0.00,  6000.00, 2, '2025-06-01T08:00:00'); -- David Wilson, USD
GO
CREATE OR ALTER TRIGGER [dbo].[trg_CardAccount_Update]
ON [dbo].[CardAccount]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[CardAccount]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[CardAccount].[CardAccountID] = inserted.[CardAccountID];
END
GO
PRINT '[dbo].[CardAccount] done'
GO









-- ==============================================================================
-- Create Card table
-- ==============================================================================
IF NOT EXISTS (SELECT TOP 1 1 FROM [INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_SCHEMA]='dbo' AND [TABLE_NAME]='Card')
EXEC ('CREATE TABLE [dbo].[Card]
(
    [CardID]            INT                         PRIMARY KEY,
    [CardAccountID]     INT             NOT NULL,
    [CardTypeID]        INT             NOT NULL,
    [CardNumber]        NVARCHAR(16)    NOT NULL    UNIQUE,
    [ActivationDate]    DATETIME2           NULL,
    [ExpirationDate]    DATE            NOT NULL,
    [CVV]               NVARCHAR(4)     NOT NULL,
    [IsActive]          BIT             NOT NULL    DEFAULT 1,
    [CreatedOn]         DATETIME2       NOT NULL    DEFAULT SYSUTCDATETIME(),
    [ModifiedOn]        DATETIME2           NULL,

    CONSTRAINT [FK_Card_CardAccount] FOREIGN KEY ([CardAccountID]) REFERENCES [dbo].[CardAccount]([CardAccountID]),
    CONSTRAINT [FK_Card_CardType]    FOREIGN KEY ([CardTypeID])    REFERENCES [dbo].[CardType]   ([CardTypeID])
)')
GO
IF (SELECT COUNT(1) FROM [dbo].[Card]) = 0
INSERT INTO [dbo].[Card] ([CardID], [CardAccountID], [CardTypeID], [CardNumber], [ActivationDate], [ExpirationDate], [CVV], [CreatedOn])
VALUES
(1, 110, 1, '4111111111111111', '2024-01-01T09:00:00', '20271231', '123', '2025-06-01T08:00:00'), -- John Doe, Visa
(2, 120, 2, '5500000000000004', '2024-05-21T10:00:00', '20261130', '456', '2025-06-01T08:00:00'), -- Jane Smith, MasterCard
(3, 130, 1, '4007000000456027', '2024-08-15T13:00:00', '20280531', '789', '2025-06-01T08:00:00'),    -- Michael Johnson, Visa
(4, 140, 3, '3782822463100305', '2024-02-03T16:00:00', '20250930', '012', '2025-06-01T08:00:00'), -- Emily Brown, American Express
(5, 150, 2, '5105105105105100', '2024-12-11T20:00:00', '20270831', '345', '2025-06-01T08:00:00') -- David Wilson, MasterCard
GO
CREATE OR ALTER TRIGGER [dbo].[trg_Card_Update]
ON [dbo].[Card]
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE [dbo].[Card]
    SET [ModifiedOn] = SYSUTCDATETIME()
    FROM inserted
    WHERE [dbo].[Card].[CardID] = inserted.[CardID];
END
GO
PRINT '[dbo].[Card] done'
GO










-- ==============================================================================
-- Create Payments table
-- ==============================================================================
DROP TABLE IF EXISTS [dbo].[Payments]
GO
CREATE TABLE [dbo].[Payments]
(
    [PaymentID]         UNIQUEIDENTIFIER                PRIMARY KEY DEFAULT NEWID(),
    [CardAccountID]     INT                 NOT NULL,
    [Amount]            DECIMAL(18,2)       NOT NULL,
    [CurrencyID]        INT                 NOT NULL,
    [PaymentDate]       DATE                NOT NULL,
    [CreatedOn]         DATETIME2           NOT NULL,

    CONSTRAINT [FK_Payments_CardAccount] FOREIGN KEY ([CardAccountID]) REFERENCES [dbo].[CardAccount]([CardAccountID]),
    CONSTRAINT [FK_Payments_Currency]    FOREIGN KEY ([CurrencyID])    REFERENCES [dbo].[Currency]([CurrencyID])
)
GO
PRINT '[dbo].[Payments] done'
GO









GO
-- ==============================================================================
-- Create Transaction table
-- ==============================================================================
DROP TABLE IF EXISTS [dbo].[Transactions]
GO
CREATE TABLE [dbo].[Transactions]
(
    [TransactionID]             UNIQUEIDENTIFIER    NOT NULL   PRIMARY KEY DEFAULT NEWID(),
    [CardID]                    INT                 NOT NULL,
    [TransactionTypeID]         INT                 NOT NULL,
    [TransactionStatusID]       INT                 NOT NULL,
    [MerchantID]                INT                 NOT NULL,
    [CurrencyID]                INT                 NOT NULL,
    [Amount]                    DECIMAL(18,2)       NOT NULL,
    [TransactionDate]           DATE                NOT NULL,
    [CreatedOn]                 DATETIME2           NOT NULL,

    CONSTRAINT [FK_Transaction_Card] FOREIGN KEY ([CardID]) REFERENCES [dbo].[Card]([CardID]),
    CONSTRAINT [FK_Transaction_TransactionType] FOREIGN KEY ([TransactionTypeID]) REFERENCES [dbo].[TransactionType]([TransactionTypeID]),
    CONSTRAINT [FK_Transaction_TransactionStatus] FOREIGN KEY ([TransactionStatusID]) REFERENCES [dbo].[TransactionStatus]([TransactionStatusID]),
    CONSTRAINT [FK_Transaction_Merchant] FOREIGN KEY ([MerchantID]) REFERENCES [dbo].[Merchant]([MerchantID]),
    CONSTRAINT [FK_Transaction_Currency] FOREIGN KEY ([CurrencyID]) REFERENCES [dbo].[Currency]([CurrencyID])
)
GO
IF (SELECT COUNT(1) FROM [dbo].[Transactions]) = 0
INSERT INTO [dbo].[Transactions] ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate], [CreatedOn])
VALUES
(1, 1, 2,  1, 1, 120.50, '2025-06-01', '2025-06-01T08:00:00')
GO
PRINT '[dbo].[Transactions] done'
GO









-- =======================================
-- Create Stored Procedure: Insert New Transaction
-- =======================================
GO
CREATE OR ALTER PROCEDURE [dbo].[usp_insert_new_transaction]
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @lasttransaction DATETIME2;
    DECLARE @newtransaction DATETIME2;
    DECLARE @minutesToAdd INT;
    DECLARE @hour INT;

    DECLARE @card INT;
    DECLARE @transactionType INT;
    DECLARE @transactionStatus INT;
    DECLARE @merchant INT;
    DECLARE @currency INT;
    DECLARE @amount DECIMAL(18,2);



    -- 1. Get the maximum [CreatedOn]
    SELECT @lasttransaction = MAX([CreatedOn]) FROM [dbo].[Transactions];


    -- 2. Add random minutes (5–20)
    IF DAY(@lasttransaction) BETWEEN 5 AND 25
        SET @minutesToAdd = 5 + ABS(CHECKSUM(NEWID())) % 16;
    ELSE
        SET @minutesToAdd = 2 + ABS(CHECKSUM(NEWID())) % 9;
    SET @newtransaction = DATEADD(MINUTE, @minutesToAdd, @lasttransaction);
    SET @newtransaction = DATEADD(SECOND, ABS(CHECKSUM(NEWID())) % 50, @newtransaction);

    -- 3. If the hour > 23, roll to next day +9h
    SET @hour = DATEPART(HOUR, @newtransaction);
    IF @hour >= 23
        SET @newtransaction = DATEADD(HOUR, 9, @newtransaction);

    -- 4. Random card between 1 and 5
    SET @card = 1 + ABS(CHECKSUM(NEWID())) % 5;

    -- 5. Random TransactionStatus between 2 and 3
    SET @transactionStatus = IIF (ABS(CHECKSUM(NEWID())) % 100 < 5, 3, 4)

    -- 6. Random Merchant between 1 and 30
    SET @merchant = 1 + ABS(CHECKSUM(NEWID())) % 30;

    -- 7. Random Currency between 1 and 2
    SELECT @currency = ca.[CurrencyID]
    FROM
        [dbo].[Card] AS c
        JOIN [dbo].[CardAccount] AS ca ON c.[CardAccountID] = ca.[CardAccountID]
    WHERE
        c.[CardID] = @card
    IF ABS(CHECKSUM(NEWID())) % 100 < 10
        SET @currency = 1 + ABS(CHECKSUM(NEWID())) % 2;

    -- 8. Random TransactionType (1 = 95%, others share 5%)
    SET @transactionType = 1

    -- 9. Random Amount between 10.00 and 1000.00
    SET @amount = CAST(10 + (ABS(CHECKSUM(NEWID())) % 991) + (ABS(CHECKSUM(NEWID())) % 100) * 0.01 AS DECIMAL(18,2));




    -- 10. Insert into Transaction table
    INSERT INTO [dbo].[Transactions]
        ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate], [CreatedOn])
    VALUES
        (@card, @transactionType, @transactionStatus, @merchant, @currency, @amount, CAST(@newtransaction AS date), @newtransaction);


    IF ABS(CHECKSUM(NEWID())) % 100 <= 2 AND @transactionStatus = 4
    BEGIN
        SET @amount = @amount * 0.2
        SET @newtransaction = DATEADD(SECOND, 1, @newtransaction)
        INSERT INTO [dbo].[Transactions]
            ([CardID], [TransactionTypeID], [TransactionStatusID], [MerchantID], [CurrencyID], [Amount], [TransactionDate], [CreatedOn])
        VALUES
            (@card, @transactionType, @transactionStatus, @merchant, @currency, @amount, CAST(@newtransaction AS date), @newtransaction);
    END


    IF MONTH(@lasttransaction) <> MONTH(@newtransaction)
    BEGIN
        INSERT INTO [dbo].[Payments] (
            [CardAccountID],
            [CurrencyID],
            [PaymentDate],
            [Amount],
            [CreatedOn]
        )
        SELECT
            ca.[CardAccountID],
            ca.[CurrencyID],
            [PaymentDate] = CAST(FORMAT(@newtransaction, 'yyyyMM') + '01' AS date),
            SUM(t.[Amount]) AS [Amount],
            @newtransaction
        FROM
            [dbo].[Transactions] AS t
            JOIN [dbo].[Card] AS c ON t.[CardID] = c.[CardID]
            JOIN [dbo].[CardAccount] AS ca ON c.[CardAccountID] = ca.[CardAccountID]
        WHERE
            t.[TransactionStatusID] IN (2,4) -- Approved or Settled
            AND CAST(FORMAT(@lasttransaction, 'yyyyMM') + '01' AS date) <= t.[TransactionDate] -- Start of last month
            AND t.[TransactionDate] < CAST(FORMAT(@newtransaction, 'yyyyMM') + '01' AS date) -- Start of current month
        GROUP BY
            ca.[CardAccountID],
            ca.[CurrencyID]
        HAVING
            SUM(t.[Amount]) > 0
    END

    

END
GO
PRINT '[dbo].[usp_insert_new_transaction] done'
GO







-- =======================================
-- Create Stored Procedure: Insert New Transaction
-- =======================================
GO
CREATE OR ALTER PROCEDURE [dbo].[usp_insert_range_transaction]
    @days int = 0
AS
BEGIN

    SET NOCOUNT ON
    DECLARE @newtransaction DATETIME2
    DECLARE @initaltime DATETIME2 = (SELECT MAX([CreatedOn]) FROM [dbo].[Transactions])
    DECLARE @continue BIT = 1


    WHILE @continue = 1
    BEGIN

        EXEC [dbo].[usp_insert_new_transaction]

        SET @newtransaction = (SELECT MAX([CreatedOn]) FROM [dbo].[Transactions])
        IF DATEDIFF(HOUR, @initaltime, @newtransaction) >= @days * 24
            SET @continue = 0
    END

END
GO
PRINT '[dbo].[usp_insert_range_transaction] done'
GO

















GO
PRINT ''
PRINT '[dbo].[usp_insert_range_transaction] started'
GO
EXEC [dbo].[usp_insert_range_transaction] @days = 10
GO


