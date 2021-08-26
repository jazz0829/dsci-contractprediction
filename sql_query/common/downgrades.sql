DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @StartDate DATE 

SET @EndDate = ?
SET @Environment = ?
SET @Product = ?
SET @StartDate = ?

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
  and NewContract.EventDate between @StartDate and @EndDate
  and NewContract.ItemType = 'Package'
  and NewContract.InflowOutflow = 'Inflow'
  and OldContract.InflowOutflow = 'Outflow'
  and oldClass.Product = @Product
  and newClass.Product = 'Accounting'
