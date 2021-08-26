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

SELECT  RTRIM(Activity.[Environment]) AS Environment    
      ,RTRIM(Activity.[AccountCode]) AS AccountCode
	  ,sum([Quantity]) AS Pageviews

FROM [domain].[ActivityDaily] AS Activity
  
LEFT JOIN (
select [Environment]
		,[AccountCode]
		,[PackageCode]
		,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
	as DateNumber
from [domain].[ActivityDaily_Contracts]
where Date <= @EndDate
)AS Contracts

ON Activity.[Environment] = Contracts.[Environment] 
AND Activity.[AccountCode] = Contracts.[AccountCode] 

LEFT  JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE Activity.[ActivityID] = 1
AND Activity.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND Activity.[Date] >= @StartDate
AND Activity.[Date] <= @EndDate
AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = @Product

GROUP BY  Activity.[Environment]      
		,Activity.[AccountCode]	