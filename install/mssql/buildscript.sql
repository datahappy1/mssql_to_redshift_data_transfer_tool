USE master;
GO

CREATE DATABASE MSSQL_to_Redshift;
GO

USE MSSQL_to_Redshift;
GO

CREATE SCHEMA mngmt;
GO

CREATE TABLE mngmt.ExecutionLogs (
	[Log_ID] INT PRIMARY KEY IDENTITY(1,1),
	[ExecutionDT] DATETIME, 
	[ExecutionStep] VARCHAR(10),
	[DatabaseName] VARCHAR(128),
	[SchemaName] VARCHAR(128),
	[TableName] VARCHAR(128),
	[TargetDirectory] VARCHAR(255),
	[Filename] VARCHAR(300), 
	[Status] CHAR(1),
	[Message] VARCHAR(MAX)
) 
GO

CREATE TABLE mngmt.ControlTable (	
	ControlTable_ID INT PRIMARY KEY IDENTITY(1,1),
	DatabaseName VARCHAR(128),
	SchemaName VARCHAR(128),
	TableName VARCHAR(128),
	ColumnName VARCHAR(128),
	Column_id SMALLINT,
	IsActive BIT
)
GO

--For DEMO purposes, let's fill the control table for the transfer with the AdventureWorks DataWarehouse tables 
--and set all columns as IsActive
USE AdventureWorksDW2016;
GO

INSERT INTO MSSQL_to_Redshift.mngmt.ControlTable ( DatabaseName, SchemaName, TableName, ColumnName, Column_id, IsActive )
SELECT	
	DB_NAME() AS DatabaseName,
	s.name AS SchemaName,
	t.name AS TableName,
	c.name AS Columnname,
	c.column_id AS Column_id,
	1 AS IsActive
FROM sys.tables t
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
INNER JOIN sys.columns c ON c.object_id = t.object_id
ORDER BY t.object_id;


USE MSSQL_to_Redshift;
GO

CREATE PROCEDURE mngmt.ExecutionLogs_Insert (
@ExecutionStep VARCHAR(10), 
@DatabaseName VARCHAR(128), 
@SchemaName VARCHAR(128), 
@TableName VARCHAR(128), 
@TargetDirectory VARCHAR(255), 
@Filename VARCHAR(300), 
@Status CHAR(1), 
@Message VARCHAR(MAX)
)
AS
BEGIN TRY
	INSERT INTO mngmt.ExecutionLogs (
		ExecutionDT, 
		ExecutionStep, 
		DatabaseName, 
		SchemaName, 
		TableName, 
		TargetDirectory, 
		[Filename], 
		[Status], 
		[Message]
		)
	SELECT 	
		GETDATE(), 
		@ExecutionStep, 
		@DatabaseName, 
		@SchemaName, 
		@TableName, 
		@TargetDirectory, 
		@Filename, 
		@Status, 
		@Message
END TRY

BEGIN CATCH
	SELECT 
		'Row not logged, MSSQL error, details:'
		+ '  Error_Number' + CAST(ERROR_NUMBER() AS VARCHAR(9))
		+ '; Error_Severity:' + CAST(ERROR_SEVERITY() AS VARCHAR(9))
		+ '; Error_State:' + CAST(ERROR_STATE() AS VARCHAR(9))
		+ '; Error_Procedure:' + ERROR_PROCEDURE() 
		+ '; Error_Line:' + CAST(ERROR_LINE() AS VARCHAR(9))
		+ '; Error_Message:' + ERROR_MESSAGE()
		AS Result;
END CATCH

GO

CREATE PROCEDURE mngmt.Extract_Filter_BCP (	
  @DatabaseName VARCHAR(128), 
  @SchemaName VARCHAR(128),  
  @TargetDirectory VARCHAR(255),
  @DryRun BIT,
  @Result VARCHAR(MAX) = '' OUTPUT
)
AS

BEGIN TRY
	---------------------
	--Declarations:
	---------------------
	DECLARE @ID SMALLINT;
	DECLARE @TableName VARCHAR(128);
	DECLARE @ColumnNamesSerialized VARCHAR(MAX);
	DECLARE @BCPCommand VARCHAR(4000);
	DECLARE @Message VARCHAR(MAX);
	DECLARE @DTNow CHAR(19) = FORMAT(GetDate(), 'yyyy_MM_dd_HH_mm_ss')
	DECLARE @Status CHAR(1);
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
		'BCP "SELECT ' + @ColumnNamesSerialized + ' FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + @DryRunQuery + ' " queryout ' + @FileName + ' -c -t, -T -S' + @@servername;
	
		DECLARE @BCPOutput TABLE (id INT IDENTITY, command NVARCHAR(256))

		INSERT INTO @BCPOutput
		EXEC master..xp_cmdshell @BCPCommand

		SET @Message =	(SELECT command FROM @BCPOutput WHERE id = (SELECT MAX(id) - 3 FROM @BCPOutput));

		SET @Status =	CASE 
							WHEN @Message LIKE '% rows copied.' 
								THEN 'S'
							ELSE 'F'
						END;

		SET @Result = @Result + '''' + @TargetDirectory + '\' + @TableName + '_' + @DTNow 
					  +	CASE 
							WHEN @ID = (SELECT MAX(RN) FROM #tmp) 
								THEN '.csv''' 
							ELSE '.csv'',' 
						END 	

        	IF @DryRun = 0
		BEGIN			
			EXEC mngmt.ExecutionLogs_Insert 'MSSQL-BCP', @DatabaseName, @SchemaName, @TableName, @TargetDirectory, @FileName, @Status, @Message
        	END
					
		DELETE FROM #tmp WHERE RN = @ID;
	END

	SELECT '(' + ISNULL(@Result,'No .csv files generated') + ')' AS Result

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
SQL Server blocked access to procedure 'sys.xp_cmdshell' of component 'xp_cmdshell' because this component is turned off as part of the security configuration for this server. A system administrator can enable the use of 'xp_cmdshell' by using sp_configure. For more information about enabling 'xp_cmdshell', search for 'xp_cmdshell' in SQL Server Books Online.

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
