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

SELECT  RTRIM(Activity.[Environment]) as Environment      
      ,RTRIM(Activity.[AccountCode]) as AccountCode
	  ,count(distinct Activity.UserID) AS ActiveUsers
	  ,count(distinct Activity.DivisionCode) As ActiveDivisions

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
AND Activity.[Date] between @StartDate and  @EndDate

AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = @Product

GROUP BY  Activity.[Environment]      
		,Activity.[AccountCode]	