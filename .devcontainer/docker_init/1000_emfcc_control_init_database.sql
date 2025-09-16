IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]='emfcc_control')
BEGIN
    EXEC('CREATE DATABASE [emfcc_control]')
    PRINT '[emfcc_control] database created'
END
GO
USE [emfcc_control]
GO
SET NOCOUNT ON
GO
IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[schemas] WHERE [name]='control')
    EXEC('CREATE SCHEMA [control] AUTHORIZATION [dbo]')
GO
IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[schemas] WHERE [name]='source')
    EXEC('CREATE SCHEMA [source] AUTHORIZATION [dbo]')
GO


-- ==============================================================================
-- Create CardType table
-- ==============================================================================
GO
DROP TABLE IF EXISTS [control].[queries_control]
GO 
CREATE TABLE [control].[queries_control]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [schema]                VARCHAR(128)    NOT NULL,
    [table]                 VARCHAR(128)    NOT NULL,
    [query_incremental]     NVARCHAR(MAX)   NOT NULL,
    [unique_keys]           NVARCHAR(MAX)   NOT NULL,
    [next_file_sequence]    BIGINT              NULL,
    [current_lsn]           BINARY(10)          NULL,
    [active]                BIT             NOT NULL    DEFAULT 1
)
GO
IF (SELECT COUNT(1) FROM [control].[queries_control]) = 0
INSERT INTO [control].[queries_control] ([connection_name],  [schema],  [table],           [query_incremental],                                                                                      [unique_keys],      [current_lsn],          [next_file_sequence]) VALUES
                                        ('basic',            'dbo',     'CardType',       'SELECT * FROM [cdc].[fn_cdc_get_all_changes_dbo_CardType](sys.fn_cdc_increment_lsn({{ current_lsn }}), {{ new_lsn }}, ''all'')',   '["CardTypeID"]',   0x00000000000000000000, 1),
                                        ('basic',            'dbo',     'Currency',       'SELECT * FROM [cdc].[fn_cdc_get_all_changes_dbo_Currency](sys.fn_cdc_increment_lsn({{ current_lsn }}), {{ new_lsn }}, ''all'')',   '["CurrencyID"]',   0x00000000000000000000, 1)
GO
PRINT '[control].[queries_control] done'
GO


GO
DROP TABLE IF EXISTS [source].[sources]
GO
CREATE TABLE [source].[sources]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [schema]                VARCHAR(128)    NOT NULL,
    [table]                 VARCHAR(128)    NOT NULL,
    [cdc_enabled]           BIT             NOT NULL    DEFAULT 0
)
GO
INSERT INTO [source].[sources]  ([connection_name], [schema],   [table],        [cdc_enabled]) VALUES
                                ('basic',           'dbo',      'example1',     0),
                                ('basic',           'dbo',      'example2',     0)


GO
DROP TABLE IF EXISTS [source].[columns]
GO
CREATE TABLE [source].[columns]
(
    [id]                    INT                        IDENTITY (1, 1) PRIMARY KEY,
    [connection_name]       VARCHAR(128)    NOT NULL,
    [schema]                VARCHAR(128)    NOT NULL,
    [table]                 VARCHAR(128)    NOT NULL,
    [column]                VARCHAR(256)    NOT NULL,
    [data_type]             VARCHAR(256)    NOT NULL,
    [unique_key]            BIT             NOT NULL    DEFAULT 0
)
GO
INSERT INTO [source].[columns]  ([connection_name], [schema],   [table],    [column],           [data_type],    [unique_key]) VALUES
                                ('basic',           'dbo',      'example1', 'id',               'INT',          1           ),
                                ('basic',           'dbo',      'example1', 'description',      'VARCHAR(20)',  0           ),
                                ('basic',           'dbo',      'example1', 'created_on',       'DATETIME2',    0           ),
                                ('basic',           'dbo',      'example1', 'modified_on',      'DATETIME2',    0           ),
                                ('basic',           'dbo',      'example1', 'version',          'ROWVERSION',   0           ),
                                ('basic',           'dbo',      'example2', 'qw_id',            'INT',          1           ),
                                ('basic',           'dbo',      'example2', 'qw_bk',            'VARCHAR(10)',  1           ),
                                ('basic',           'dbo',      'example2', 'qw_description',   'VARCHAR(20)',  0           ),
                                ('basic',           'dbo',      'example2', 'qw_created_on',    'DATETIME2',    0           ),
                                ('basic',           'dbo',      'example2', 'qw_modified_on',   'DATETIME2',    0           ),
                                ('basic',           'dbo',      'example2', 'qw_version',       'ROWVERSION',   0           )
GO


GO

CREATE OR ALTER VIEW [control].[v_queries]
AS
    SELECT
        [id],
        [connection_name],
        [schema],
        [table],
        [query_incremental],
        [unique_keys],           
        [next_file_sequence],    
        [current_lsn],
        [active],
        [next_file_sequence_txt] = FORMAT([next_file_sequence], '00000000000000000000') + '.parquet'
    FROM [control].[queries_control]
GO


CREATE OR ALTER VIEW [source].[v_sources]
AS
    SELECT
        src.[id],
        src.[connection_name],
        src.[schema],
        src.[table],
        src.[cdc_enabled],
        col.[unique_keys]
    FROM
        [source].[sources] AS src
        LEFT JOIN (
            SELECT
                [connection_name],
                [schema],
                [table],
                [unique_keys] = '[' + STRING_AGG(CAST(IIF([unique_key] = 1, '"' + [column] + '"', NULL) AS NVARCHAR(max)), ', ') + ']'
            FROM [source].[columns]
            GROUP BY
                [connection_name],
                [schema],
                [table]
        ) AS col
            ON  src.[connection_name] = col.[connection_name]
            AND src.[schema] = col.[schema]
            AND src.[table] = col.[table]
GO




CREATE OR ALTER PROC [control].[usp_add_source_object] 
    @id INT
AS
BEGIN


    INSERT INTO [control].[queries_control] (
        [connection_name],
        [schema],
        [table],
        [query_incremental],
        [unique_keys],
        [current_lsn],
        [next_file_sequence]
    )
    SELECT  
        [connection_name] = src.[connection_name],
        [schema],
        [table],
        [query_incremental] = 'SELECT * FROM [cdc].[fn_cdc_get_all_changes_' + REPLACE(REPLACE(REPLACE(src.[schema] + '_' + src.[table], '[', ''), ']', ''), '.', '_') + '](sys.fn_cdc_increment_lsn({{ current_lsn }}), {{ new_lsn }}, ''all'')',
        [unique_keys] = src.[unique_keys],
        [current_lsn] = 0x00000000000000000000,
        [next_file_sequence] = 1
    FROM [source].[v_sources] AS src
    WHERE src.[id] = @id

END
GO




CREATE OR ALTER PROC [control].[usp_refresh_metadata] 
AS
BEGIN


    MERGE [source].[sources] AS tgt
        USING (
                SELECT 
                    [schema] = s.[name],
                    [table] = t.[name],
                    [cdc_enabled] = t.[is_tracked_by_cdc] 
                FROM 
                    [emfcc_source_cdc].[sys].[schemas] AS s 
                    JOIN [emfcc_source_cdc].[sys].[tables] AS t 
                        ON t.[schema_id] = s.[schema_id] 
                WHERE s.[name] NOT IN ('sys', 'cdc', 'information_schema') AND t.[name] NOT IN ('systranschemas')

            ) AS src
            ON  tgt.[schema] = src.[schema]
            AND tgt.[table] = src.[table]
        
        WHEN NOT MATCHED BY TARGET
        THEN INSERT ([connection_name], [schema],       [table],        [cdc_enabled])
             VALUES ('basic',           src.[schema],   src.[table],    [cdc_enabled])
        
        WHEN MATCHED
        THEN UPDATE SET [cdc_enabled] = src.[cdc_enabled]
        
        WHEN NOT MATCHED BY SOURCE 
        THEN DELETE;   


    MERGE [source].[columns] AS tgt
        USING (
                SELECT  
                    [schema] = c.[TABLE_SCHEMA],
                    [table] = c.[TABLE_NAME],
                    [column] = c.[COLUMN_NAME],
                    [data_type] = c.[DATA_TYPE],
                    [unique_key] = ISNULL(u.[unique_key], CAST(0 AS BIT))
                FROM 
                    [emfcc_source_cdc].[INFORMATION_SCHEMA].[COLUMNS] AS c
                    LEFT JOIN (
                        SELECT 
                            tc.[TABLE_SCHEMA],
                            tc.[TABLE_NAME],
                            kcu.[COLUMN_NAME],
                            [unique_key] = CAST(1 AS BIT)
                        FROM 
                            [emfcc_source_cdc].[INFORMATION_SCHEMA].[TABLE_CONSTRAINTS] tc
                            JOIN [emfcc_source_cdc].[INFORMATION_SCHEMA].[KEY_COLUMN_USAGE] kcu
                                ON tc.[CONSTRAINT_NAME] = kcu.[CONSTRAINT_NAME]
                                AND tc.[TABLE_SCHEMA] = kcu.[TABLE_SCHEMA]
                                AND tc.[TABLE_NAME] = kcu.[TABLE_NAME]
                        WHERE tc.[CONSTRAINT_TYPE] = 'PRIMARY KEY'
                    ) AS u
                    ON  u.[TABLE_SCHEMA] = c.[TABLE_SCHEMA]
                    AND u.[TABLE_NAME] = c.[TABLE_NAME]
                    AND u.[COLUMN_NAME] = c.[COLUMN_NAME]
                WHERE 
                    c.[TABLE_SCHEMA] NOT IN ('sys', 'cdc', 'information_schema') 
                    AND c.[TABLE_NAME] NOT IN ('systranschemas')
            ) AS src
                ON  tgt.[schema] = src.[schema]
                AND tgt.[table] = src.[table]
                AND tgt.[column] = src.[column]
        
        WHEN NOT MATCHED BY TARGET
        THEN INSERT ([connection_name], [schema],       [table],       [column],       [data_type],        [unique_key])
             VALUES ('basic',           src.[schema],   src.[table],   src.[column],   src.[data_type],    src.[unique_key])
        
        WHEN NOT MATCHED BY SOURCE
        THEN DELETE;

END
GO

