DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)



SET @EndDate = ?
SET @Environment = ?
SET @Product = ?

DECLARE @PredictDate DATE 
SET @PredictDate = (SELECT top 1 cast(PredictDate as date) as PredictDate
	FROM [CustomerIntelligence].[publish].[ISUsageScore]
	LEFT JOIN [domain].[PackageClassification] AS PackageClassification
	ON [ISUsageScore].[Environment] = PackageClassification.[Environment] 
	AND [ISUsageScore].[PackageCode] = PackageClassification.[PackageCode]
	where Product = @Product
	and PredictDate <= @EndDate
	and ISUsageScore.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
	order by PredictDate desc)




SELECT ISUS.AccountCode
	   ,ISUS.Environment
	   ,ISUS.Score
	   ,PackageClassification.Product
	   ,ISUS.Phase
	   ,cast (ISUS.PredictDate as date) as PredictDate
	   ,DG.DowngradeDate
from [CustomerIntelligence].[publish].[ISUsageScore] as ISUS

left join [domain].[PackageClassification] AS PackageClassification
ON ISUS.[Environment] = PackageClassification.[Environment] 
AND ISUS.[PackageCode] = PackageClassification.[PackageCode]

-- get who downgraded
left join (
SELECT distinct RTRIM(NewContract.[Environment]) AS Environment
      ,RTRIM(NewContract.[AccountCode]) AS AccountCode
      ,NewContract.[EventDate] as DowngradeDate
      
  FROM [CustomerIntelligence].[domain].[Contracts] as NewContract
  left join [CustomerIntelligence].[domain].[Contracts] as OldContract
  on NewContract.AccountCode = OldContract.AccountCode
  and NewContract.Environment = OldContract.Environment
  and NewContract.EventDate = OldContract.EventDate
  and NewContract.ItemType = OldContract.ItemType
  and NewContract.EventType = OldContract.EventType

  left join [CustomerIntelligence].[domain].[PackageClassification] as newClass
  on newClass.Environment = NewContract.Environment
  and newClass.PackageCode = NewContract.PackageCode

  left join [CustomerIntelligence].[domain].[PackageClassification] as oldClass
  on oldClass.Environment = OldContract.Environment
  and oldClass.PackageCode = OldContract.PackageCode

  where NewContract.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
  and NewContract.EventType = 'CDN'
  and NewContract.EventDate > @PredictDate
  and NewContract.EventDate < dateadd(month, 3, @PredictDate)

  and NewContract.ItemType = 'Package'
  and NewContract.InflowOutflow = 'Inflow'
  and OldContract.InflowOutflow = 'Outflow'
  and oldClass.Product = @Product
  and newClass.Product = 'Accounting'
  ) as DG

  on ISUS.AccountCode = DG.AccountCode
  and ISUS.Environment = DG.Environment

  -- join the scores from the right date
  WHERE cast(ISUS.PredictDate as date) = @PredictDate
	
and Product = @Product


