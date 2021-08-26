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

SELECT RTRIM(Activity.[Environment]) as Environment
      ,RTRIM(Activity.[AccountCode]) as AccountCode
	   ,sum([Quantity]) AS UseMobile
  FROM [domain].[ActivityDaily] as Activity

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

JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE Activity.[ActivityID] = 8
AND Activity.[Date] >= @StartDate 
AND Activity.[Date] <= @EndDate
AND Activity.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = @Product

GROUP BY Activity.[Environment]
    ,Activity.[AccountCode]