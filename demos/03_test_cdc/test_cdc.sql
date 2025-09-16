
IF NOT EXISTS(SELECT TOP 1 1 FROM [sys].[configurations] WHERE [name]='clr enabled' AND CAST([value] AS BIT)=1)
BEGIN
    EXECUTE [sys].[sp_configure] 'clr enabled', 1;
    RECONFIGURE;
    PRINT 'CLR enabled'
END
GO
IF NOT EXISTS (SELECT TOP 1 1 FROM [sys].[databases] WHERE [name]=DB_NAME() AND [is_cdc_enabled]=1)
BEGIN
    EXEC [sys].[sp_cdc_enable_db]
    PRINT 'CDC enabled '
END
GO


CREATE TABLE [dbo].[Employees] (
    [EmpID] 	INT 	        PRIMARY KEY,
    [Name] 	    NVARCHAR(50),
    [Salary] 	INT
);

EXEC [sys].[sp_cdc_enable_table]
     @source_schema = N'dbo',
     @source_name = N'Employees',
     @role_name = NULL,
     @supports_net_changes = 1



SELECT
    s.[name] AS [schema],
    t.[name] AS [table],
    t.[is_tracked_by_cdc]
FROM
    [sys].[tables] AS t
    JOIN [sys].[schemas] AS s
        ON t.[schema_id] = s.[schema_id]
ORDER BY 1, 2



INSERT INTO [dbo].[Employees] ([EmpID], [Name], [Salary]) VALUES (1, 'Alice', 5000);
INSERT INTO [dbo].[Employees] ([EmpID], [Name], [Salary]) VALUES (2, 'Pablo', 4800);

UPDATE [dbo].[Employees] SET [Salary] = 6000 WHERE [EmpID] = 1;
UPDATE [dbo].[Employees] SET [Salary] = 7000 WHERE [EmpID] = 1;
UPDATE [dbo].[Employees] SET [Salary] = 5800, [Name]='Pablo R.' WHERE [EmpID] = 2;

DELETE FROM [dbo].[Employees] WHERE [EmpID] = 1;



SELECT __$start_lsn, [EmpID], [Name], [Salary], [__$operation]
FROM [cdc].[fn_cdc_get_all_changes_dbo_Employees](
    [sys].[fn_cdc_get_min_lsn]('dbo_Employees'),
    [sys].[fn_cdc_get_max_lsn](),
    'all'
)


SELECT __$start_lsn, [EmpID], [Name], [Salary], [__$operation]
FROM [cdc].[fn_cdc_get_net_changes_dbo_Employees](
    [sys].[fn_cdc_get_min_lsn]('dbo_Employees'),
    [sys].[fn_cdc_get_max_lsn](),
    'all'
);


/*


SELECT __$start_lsn,  [EmpID], [Name], [Salary], [__$operation]
FROM [cdc].[fn_cdc_get_all_changes_dbo_Employees](
    0x00000034000021100003,
    [sys].[fn_cdc_get_max_lsn](),
    'all'
)

SELECT __$start_lsn, [EmpID], [Name], [Salary], [__$operation]
FROM [cdc].[fn_cdc_get_net_changes_dbo_Employees](
    0x00000034000021100003,
    [sys].[fn_cdc_get_max_lsn](),
    'all'
);


*/
