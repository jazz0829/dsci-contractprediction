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

Select RTRIM(ActivityDaily.AccountCode) AS AccountCode
	 , RTRIM(ActivityDaily.Environment ) AS Environment
	 , ContinuousMonitoring
	 , Scanning
	 , ProcessManagement

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

-- JOIN Continous Monitoring
LEFT JOIN 
(SELECT AccountCode, Environment, SUM(Quantity) as ContinuousMonitoring
From [domain].[ActivityDaily] as ActivityDaily

Where ActivityDaily.ActivityID IN('5017','5018','5019')
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
Group by AccountCode, Environment


) CM
ON CM.AccountCode=ActivityDaily.AccountCode AND CM.Environment=ActivityDaily.Environment 

-- JOIN Scan Service
LEFT JOIN 
(SELECT AccountCode, Environment, SUM(Quantity) as Scanning
From [domain].[ActivityDaily] as ActivityDaily 

Where ActivityDaily.ActivityID in (5003, 5005) 
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
Group by AccountCode, Environment


) SR
ON SR.AccountCode=ActivityDaily.AccountCode AND SR.Environment=ActivityDaily.Environment 


--JOIN Process management
LEFT JOIN
(SELECT AccountCode, Environment, SUM(Quantity) as ProcessManagement
From [domain].[ActivityDaily] as ActivityDaily 

Where ActivityDaily.ActivityID in (5004)
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
Group by AccountCode, Environment


) PM
ON PM.AccountCode=ActivityDaily.AccountCode AND PM.Environment=ActivityDaily.Environment 

Where ActivityDaily.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
and ActivityDaily.[Date] >= @StartDate
and ActivityDaily.[Date] <= @EndDate
AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = 'Accountancy'

Group by ActivityDaily.AccountCode, ActivityDaily.Environment, ContinuousMonitoring, Scanning, ProcessManagement
order by AccountCode