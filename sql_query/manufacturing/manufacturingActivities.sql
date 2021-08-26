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

Select RTRIM(ActivityDaily.AccountCode) AccountCode
	 , RTRIM(ActivityDaily.Environment) Environment
	 , ManufacturingBasic
	 , ManufacturingAdvanced


From [domain].[ActivityDaily] as ActivityDaily

LEFT JOIN (
select [Environment]
		,[AccountCode]
		,[PackageCode]
		,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
	as DateNumber
from [domain].[ActivityDaily_Contracts]
where Date <= @EndDate
)AS Contracts

ON ActivityDaily.[Environment] = Contracts.[Environment] 
AND ActivityDaily.[AccountCode] = Contracts.[AccountCode] 

JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

-- JOIN BASIC QUANTITY
LEFT JOIN 
(SELECT AccountCode, Environment, SUM(Quantity) as ManufacturingBasic
From [domain].[ActivityDaily] as ActivityDaily

Where (ActivityDaily.ActivityID in (5175, 5060, 5061, 5062, 5058, 5173, 5261, 5259))
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
Group by AccountCode, Environment


) BAS
ON BAS.AccountCode=ActivityDaily.AccountCode AND BAS.Environment=ActivityDaily.Environment 

-- JOIN ADVANCED QUANTITY
LEFT JOIN 
(SELECT AccountCode, Environment, SUM(Quantity) as ManufacturingAdvanced
From [domain].[ActivityDaily] as ActivityDaily 

Where  (ActivityDaily.ActivityID in (5044, 5045, 5046, 5056, 5168))
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
Group by AccountCode, Environment


) ADV
ON ADV.AccountCode=ActivityDaily.AccountCode AND ADV.Environment=ActivityDaily.Environment 



Where ActivityDaily.Environment = @Environment
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = 'Manufacturing'

Group by ActivityDaily.AccountCode, ActivityDaily.Environment, ManufacturingBasic, ManufacturingAdvanced
order by AccountCode