DECLARE @EndDate DATE 
DECLARE @Window INT  
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @StartDate DATE 

SET @EndDate = ?
SET @Window = ?
SET @Environment = ?
SET @Product = ?
SET @StartDate = DATEADD(DAY, -@Window, @EndDate)

SELECT    
	RTRIM(Contracts.[AccountCode]) as AccountCode
    ,RTRIM(Contracts.[Environment]) as Environment
    , SUM(ProjectTotalCostEntries) AS ProjectTotalCostEntriesCount
    , SUM(ProjectTypeFixedPrice) AS ProjectTypeFixedPrice
    , SUM(ProjectTypeTimeAndMaterial) AS ProjectTypeTimeAndMaterial
    , SUM(ProjectTypeNonBillable) AS ProjectTypeNonBillable 
	, SUM(ProjectTypePrepaidRetainer) AS ProjectTypePrepaidRetainer
	, SUM(ProjectTypePrepaidHTB) AS ProjectTypePrepaidHTB

FROM domain.DivisionStatistics_DailyChanges  DSDC

INNER JOIN domain.Divisions D
    ON D.Environment = DSDC.Environment
    AND D.DivisionCode = DSDC.DivisionCode


LEFT JOIN (
select [Environment]
              ,[AccountCode]
              ,[PackageCode]
              ,[AccountID]
              ,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
       as DateNumber
from [domain].[ActivityDaily_Contracts]
where Date <= @EndDate
)AS Contracts

ON D.[AccountID] = Contracts.[AccountID] 

LEFT  JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE PackageClassification.[Product] = @Product
AND Contracts.[DateNumber] = 1
and D.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
and (D.Deleted IS NULL OR D.Deleted > @EndDate)           
AND D.BlockingStatusCode < 100 
AND DSDC.Date BETWEEN  @StartDate AND @EndDate  

             
GROUP BY
   Contracts.AccountCode
   ,Contracts.[Environment]