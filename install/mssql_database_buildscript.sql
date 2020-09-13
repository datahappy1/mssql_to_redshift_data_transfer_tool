USE master;
GO

CREATE DATABASE MSSQL_to_Redshift;
GO

USE MSSQL_to_Redshift;
GO

CREATE SCHEMA mngmt;
GO

CREATE TABLE mngmt.ControlTable
(
    ControlTable_ID INT PRIMARY KEY IDENTITY (1,1),
    DatabaseName    VARCHAR(128),
    SchemaName      VARCHAR(128),
    TableName       VARCHAR(128),
    ColumnName      VARCHAR(128),
    Column_id       SMALLINT,
    IsActive        BIT
)
GO

--For Integration testing purpose, let's insert 2 rows with test source data
INSERT INTO mngmt.ControlTable(DatabaseName, SchemaName, TableName, ColumnName, Column_id, IsActive)
SELECT 'MSSQL_to_Redshift', 'mngmt', 'Integration_test_table', 'test_column_1', 1, 1
UNION ALL
SELECT 'MSSQL_to_Redshift', 'mngmt', 'Integration_test_table', 'test_column_2', 2, 1;
GO
--Lets also create now this integration testing source table
CREATE TABLE mngmt.Integration_test_table
(
    test_column_1 VARCHAR(50),
    test_column_2 VARCHAR(50)
)
GO
--Lets insert a sample row to this table
INSERT INTO mngmt.Integration_test_table(test_column_1, test_column_2)
SELECT 'test_column_1_value', 'test_column_2_value'


--For DEMO purposes, let's fill the control table for the transfer with the AdventureWorks DataWarehouse tables 
--and set all columns as IsActive
USE AdventureWorksDW2016;
GO

INSERT INTO MSSQL_to_Redshift.mngmt.ControlTable (DatabaseName, SchemaName, TableName, ColumnName, Column_id, IsActive)
SELECT DB_NAME()   AS DatabaseName,
       s.name      AS SchemaName,
       t.name      AS TableName,
       c.name      AS Columnname,
       c.column_id AS Column_id,
       1           AS IsActive
FROM sys.tables t
         INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
         INNER JOIN sys.columns c ON c.object_id = t.object_id
ORDER BY t.object_id;


USE MSSQL_to_Redshift;
GO

CREATE PROCEDURE mngmt.Extract_Filter_BCP(
    @DatabaseName VARCHAR(128),
    @SchemaName VARCHAR(128),
    @TargetDirectory VARCHAR(255),
    @DryRun BIT
)
AS

SET NOCOUNT ON;

BEGIN TRY
	---------------------
	--Declarations:
	---------------------
	DECLARE @ID SMALLINT;
	DECLARE @TableName VARCHAR(128);
	DECLARE @ColumnNamesSerialized VARCHAR(MAX);
	DECLARE @BCPCommand VARCHAR(4000);
	DECLARE @DTNow CHAR(19) = FORMAT(GetDate(), 'yyyy_MM_dd_HH_mm_ss')
	DECLARE @FileName VARCHAR(300);
	DECLARE @DryRunQuery VARCHAR(10) = '';

	---------------------
	--Execution:
	---------------------
	SELECT i.*
	INTO #tmp
	FROM (
		SELECT	ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RN,
				DatabaseName,
				SchemaName,
				TableName,
				STUFF(
						(	SELECT ', ' + Columnname
							FROM mngmt.ControlTable i
							WHERE	i.DatabaseName = o.DatabaseName
								AND i.SchemaName = o.SchemaName
								AND i.TableName = o.TableName
								AND IsActive = 1
							ORDER BY i.Column_id
							FOR XML PATH ('')), 1, 1, ''
					) AS ColumnNamesSerialized
		FROM mngmt.ControlTable o
		WHERE	DatabaseName = @DatabaseName
			AND SchemaName = @SchemaName
		GROUP BY DatabaseName,SchemaName,TableName
	) i
	WHERE i.ColumnNamesSerialized IS NOT NULL;

	WHILE EXISTS (SELECT * FROM #tmp)
	BEGIN

		SELECT TOP(1)
		@ID = RN,
		@DatabaseName = DatabaseName,
		@SchemaName = Schemaname,
		@TableName = TableName,
		@ColumnNamesSerialized = ColumnNamesSerialized
		FROM #tmp
		ORDER BY RN;

		SET @FileName = @TargetDirectory + '\' + @TableName + '_' + @DTNow + '.csv';

		IF @DryRun = 1
		BEGIN
			SET @DryRunQuery = ' WHERE 1=0'
		END

		SELECT @BCPCommand =
		'BCP "SELECT ' + @ColumnNamesSerialized + ' FROM ' + @DatabaseName + '.' + @SchemaName + '.' +
		 @TableName + @DryRunQuery + ' " queryout ' + @FileName + ' -c -t, -T -S' + @@servername;

		DECLARE @BCPOutput TABLE (id INT IDENTITY, command NVARCHAR(256))

		INSERT INTO @BCPOutput
			EXEC master..xp_cmdshell @BCPCommand

		DECLARE @Output TABLE (filepath NVARCHAR(256))

		INSERT INTO @Output
			SELECT @TargetDirectory + '\' + @TableName + '_' + @DTNow + '.csv'

		DELETE FROM #tmp WHERE RN = @ID;

	END

	SELECT * FROM @Output;

END TRY

BEGIN CATCH

	SELECT
		'MSSQL error, details:'
		+ '  Error_Number' + CAST(ERROR_NUMBER() AS VARCHAR(9))
		+ '; Error_Severity:' + CAST(ERROR_SEVERITY() AS VARCHAR(9))
		+ '; Error_State:' + CAST(ERROR_STATE() AS VARCHAR(9))
		+ '; Error_Procedure:' + ERROR_PROCEDURE()
		+ '; Error_Line:' + CAST(ERROR_LINE() AS VARCHAR(9))
		+ '; Error_Message:' + ERROR_MESSAGE()
		AS Result;

END CATCH

GO

/* Possible error message while running xp_cmdshell for the first time:
   Msg 15281, Level 16, State 1, Procedure master..xp_cmdshell, Line 1 [Batch Start Line 2]
   SQL Server blocked access to procedure 'sys.xp_cmdshell' of component 'xp_cmdshell' because this component
   is turned off as part of the security configuration for this server. A system administrator can enable the use of
   'xp_cmdshell' by using sp_configure. For more information about enabling 'xp_cmdshell',
   search for 'xp_cmdshell' in SQL Server Books Online.

--Solution is:
EXEC sp_configure 'show advanced options', 1
GO
RECONFIGURE
GO
EXEC sp_configure 'xp_cmdshell', 1  
GO
RECONFIGURE
GO
*/
