USE SQLR;
GO


-- must have a write permissions on folder: C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData
DECLARE @RScript nvarchar(max)
SET @RScript = N'library(RevoScaleR)
				rxOptions(sampleDataDir = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData")
				inFile <- file.path(rxGetOption("sampleDataDir"), "airsample.csv")
				of <-  rxDataStep(inData = inFile, outFile = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData/airline20170428_2.xdf", 
							 transformVars = c("ArrDelay", "CRSDepTime","DayOfWeek")
							,transforms = list(ArrDelay = as.integer(ArrDelay), CRSDepTime = as.numeric(CRSDepTime), DayOfWeek = as.character(DayOfWeek))
							,overwrite = TRUE
							,maxRowsByCols = 10000000
							,rowsPerRead = 200000)
				OutputDataSet <- rxXdfToDataFrame(of)'

DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT 1 AS N'

EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
WITH RESULT SETS (
					(
					 ArrDelay INT
					,CRSDepTime DECIMAL(6,4)
					,DofWeek NVARCHAR(20)
					)
					)
GO


--------------------
--Complete process
--------------------
CREATE TABLE AirFlights_small 
(id INT IDENTITY(1,1)
,ArrDelay INT
,CRSDepTime DECIMAL(6,4)
,DofWeek NVARCHAR(20) 
);
GO


CREATE Procedure ImportXDFtoSQLTable
AS
DECLARE @RScript nvarchar(max)
SET @RScript = N'library(RevoScaleR)
				rxOptions(sampleDataDir = "C:/Program Files/Microsoft SQL Server/130/R_SERVER/library/RevoScaleR/SampleData")
				inFile <- file.path(rxGetOption("sampleDataDir"), "airsample.csv")
				of <-  rxDataStep(inData = inFile, outFile = "airline20170428_2.xdf", 
				transformVars = c("ArrDelay", "CRSDepTime","DayOfWeek")
			,transforms = list(ArrDelay = as.integer(ArrDelay), CRSDepTime = as.numeric(CRSDepTime), DayOfWeek = as.character(DayOfWeek))
			,overwrite = TRUE
			,maxRowsByCols = 10000000)
				OutputDataSet <- data.frame(rxReadXdf(file=of, varsToKeep=c("ArrDelay", "CRSDepTime","DayOfWeek")))'

DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT 1 AS N'
EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
WITH RESULT SETS ((ArrDelay INT,CRSDepTime DECIMAL(6,4),DofWeek NVARCHAR(20)));
GO


INSERT INTO AirFlights_small
EXECUTE ImportXDFtoSQLTable;
GO



SELECT count(*) FROM AirFlights_small


-- COMPARISON
---------------------
-- Normal T-SQL Query
-- For the baseline
---------------------
SET STATISTICS TIME ON;

SELECT 
[DofWeek]
,AVG(ArrDelay) AS [means]
FROM
	AirFlights_small
GROUP BY 
	[DofWeek]

SET STATISTICS TIME OFF;

--------------------
-- ColumnStore table
--------------------

CREATE TABLE AirFlights_CS
(id INT IDENTITY(1,1)
,ArrDelay INT
,CRSDepTime DECIMAL(6,4)
,DofWeek NVARCHAR(20) 
);
GO

INSERT INTO AirFlights_CS(ArrDelay, CRSDepTime, DofWeek)
SELECT ArrDelay, CRSDepTime, DofWeek FROM AirFlights_small 


CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_AirFlight
ON AirFlights_CS
(id, ArrDelay, CRSDepTime, DofWeek);
GO


SET STATISTICS TIME ON;

SELECT 
[DofWeek]
,AVG(ArrDelay) AS [means]
FROM
	AirFlights_CS
GROUP BY 
	[DofWeek]

SET STATISTICS TIME OFF;

-- Clean index
DROP INDEX [NCCI_AirFlight] ON [dbo].AirFlights_CS
GO

--------------------
-- Memory-optimized table
--------------------
ALTER DATABASE SQLR ADD FILEGROUP sqlr_mod CONTAINS MEMORY_OPTIMIZED_DATA   
ALTER DATABASE SQLR ADD FILE (name='sqlr_mod1', filename='C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\sqlr_mod1') TO FILEGROUP sqlr_mod   
ALTER DATABASE SQLR SET MEMORY_OPTIMIZED_ELEVATE_TO_SNAPSHOT=ON  
GO  

-- DROP TABLE dbo.AirFlight_M   
CREATE TABLE dbo.AirFlight_M   
(  
  id INT NOT NULL PRIMARY KEY NONCLUSTERED
 ,ArrDelay INT
 ,CRSDepTime DECIMAL(6,4) 
 ,DofWeek NVARCHAR(20)
) WITH (MEMORY_OPTIMIZED=ON, DURABILITY = SCHEMA_AND_DATA);
GO

--Insert Values into AirFlight_M
INSERT INTO AirFlight_M
SELECT * FROM AirFlights_small 
-- (8400013 row(s) affected)



-- Run the statistics
SET STATISTICS TIME ON;

SELECT 
[DofWeek]
,AVG(ArrDelay) AS [means]
FROM
	AirFlight_M
GROUP BY 
	[DofWeek]

SET STATISTICS TIME OFF;

-- DROP PROCEDURE calculateAveragePerDay  
CREATE PROCEDURE calculateAveragePerDay  
    WITH  
        NATIVE_COMPILATION,  
        SCHEMABINDING  
AS  
BEGIN ATOMIC  
    WITH  
        (TRANSACTION ISOLATION LEVEL = SNAPSHOT,  LANGUAGE = N'us_english')  

	  SELECT 
		[DofWeek]
		,AVG(ArrDelay) AS [means]
	FROM
		dbo.AirFlight_M
	GROUP BY 
		[DofWeek];  

END; 


EXECUTE calculateAveragePerDay;
GO


------------------------------
--
-- Running Logistic regression
--
------------------------------


SET STATISTICS TIME ON;
-- 1. T-SQL
DECLARE @RScript nvarchar(max)
SET @RScript = N'library(RevoScaleR)
				LMResults <- rxLinMod(ArrDelay ~ DofWeek, data = InputDataSet)
				OutputDataSet <- data.frame(LMResults$coefficients)'
DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT ArrDelay, DofWeek FROM [dbo].[AirFlights_small]'
EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
WITH RESULT SETS ((
			--DofWeek NVARCHAR(20)
		--	,
			Coefficient DECIMAL(10,5)
			));
GO
SET STATISTICS TIME OFF;


SET STATISTICS TIME ON;
-- 2. ColumnStore
DECLARE @RScript nvarchar(max)
SET @RScript = N'library(RevoScaleR)
				LMResults <- rxLinMod(ArrDelay ~ DofWeek, data = InputDataSet)
				OutputDataSet <- data.frame(LMResults$coefficients)'
DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT ArrDelay, DofWeek FROM [dbo].[AirFlights_CS]'
EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
WITH RESULT SETS ((
			--DofWeek NVARCHAR(20)
		--	,
			Coefficient DECIMAL(10,5)
			));
GO
SET STATISTICS TIME OFF;


SET STATISTICS TIME ON;
-- 3. Memory optimized
DECLARE @RScript nvarchar(max)
SET @RScript = N'library(RevoScaleR)
				LMResults <- rxLinMod(ArrDelay ~ DofWeek, data = InputDataSet)
				OutputDataSet <- data.frame(LMResults$coefficients)'
DECLARE @SQLScript nvarchar(max)
SET @SQLScript = N'SELECT ArrDelay, DofWeek FROM [dbo].[AirFlight_M]'
EXECUTE sp_execute_external_script
	 @language = N'R'
	,@script = @RScript
	,@input_data_1 = @SQLScript
WITH RESULT SETS ((
			--DofWeek NVARCHAR(20)
		--	,
			Coefficient DECIMAL(10,5)
			));
GO
SET STATISTICS TIME OFF;