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
	[FilenameOut] VARCHAR(300), 
	[Status] CHAR(1),
	[Message] VARCHAR(MAX)
) 
GO

CREATE TABLE mngmt.ControlTable (	
	ControlTable_ID INT PRIMARY KEY IDENTITY(1,1),
	DatabaseName VARCHAR(255),
	SchemaName VARCHAR(255),
	TableName VARCHAR(255),
	ColumnName VARCHAR(255),
	Column_id INT,
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

CREATE PROCEDURE mngmt.Extract_Filter_BCP (	
  @DatabaseName VARCHAR(255), 
  @SchemaName VARCHAR(255),  
  @TargetDirectory VARCHAR(255)
)
AS

---------------------
--Declarations:
---------------------
DECLARE @ID SMALLINT;
DECLARE @TableName VARCHAR(255);
DECLARE @ColumnNamesSerialized VARCHAR(MAX);
DECLARE @BCPCommand VARCHAR(4000);
DECLARE @Message NVARCHAR(256);
DECLARE @DTNow CHAR(19) = FORMAT(GetDate(), 'yyyy_MM_dd_HH_mm_ss')

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
BEGIN TRY
 
	SELECT TOP(1) 
	@ID = RN,
	@DatabaseName = DatabaseName,
	@SchemaName = Schemaname,
	@TableName = TableName,
	@ColumnNamesSerialized = ColumnNamesSerialized
	FROM #tmp
	ORDER BY RN;
 
	SELECT @BCPCommand =
	'BCP "SELECT ' + @ColumnNamesSerialized + ' FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName +'" queryout ' + @TargetDirectory + '\' + @TableName + '_' + @DTNow + '.csv -c -t, -T -S' + @@servername;
	
	DECLARE @BCPOutput TABLE (id INT IDENTITY, command NVARCHAR(256))

	INSERT INTO @BCPOutput
	EXEC master..xp_cmdshell @BCPCommand

	SET @Message =	(SELECT command FROM @BCPOutput WHERE id = (SELECT MAX(id) - 3 FROM @BCPOutput));

	INSERT INTO mngmt.ExecutionLogs (ExecutionDT, FilenameOut, [Status], [Message]) 
		SELECT	GETDATE() AS ExecutionDT, 
				@TargetDirectory + '\' + @TableName + '_' + @DTNow + '.csv' AS FilenameOut,
				CASE 
					WHEN @Message LIKE '% rows copied.' THEN 'S'
					ELSE 'F'
				END AS [Status],
				@Message AS [Message]

	DELETE FROM #tmp WHERE RN = @ID;

END TRY
BEGIN CATCH

  SELECT 
		 ERROR_NUMBER() AS ErrorNumber
		,ERROR_SEVERITY() AS ErrorSeverity
		,ERROR_STATE() AS ErrorState
		,ERROR_PROCEDURE() AS ErrorProcedure
		,ERROR_LINE() AS ErrorLine
		,ERROR_MESSAGE() AS ErrorMessage;

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
