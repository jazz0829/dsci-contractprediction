DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(25)
DECLARE @Product VARCHAR(30)

SET @EndDate = ?
SET @Environment = ?
SET @Product = ? ;

WITH ORDERED AS
(
select		[Environment]
			,[Date] as LastContractUpgradeDate
			,[AccountCode]
			,[ContractEventTypeCode]
			,[PackageCode]
			,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
		as DateNumber
	from [domain].[ActivityDaily_Contracts]
	where Date <= @EndDate
		  and ContractEventTypeCode = 'CUP')

SELECT RTRIM(Accounts.[Environment]) as Environment
	  ,RTRIM(Accounts.[AccountCode]) as AccountCode
	  ,Accounts.[Name] AS AccountName
	  ,Accounts.[IsAnonymized]
	  ,Accounts.[AccountClassificationCode]
	  ,Accounts.[SectorCode]
	  ,Contracts.[ContractEventTypeCode]
	  ,Contracts.[PackageCode]
	  ,Contracts.[NumberOfUsers]
	  ,Contracts.[NumberOfAdministrations]
	  ,Contracts.[AccountantOrLinked]
	  ,Contracts.[MRR]
	  ,Contracts1.[LastContractUpgradeDate]
	  ,PackageClassification.[Product]
	  ,PackageClassification.[Edition]
	  ,PackageClassification.[Solution]
	  ,Summary.[FirstCommStartDate]
	  ,Summary.[LatestCommFinalDate]
	  ,Summary.[FirstTrialStartDate]
	  ,Summary.[LatestTrialFinalDate]
	  ,Summary.[FullCancellationRequestDate]

  FROM [domain].[Accounts] as Accounts

LEFT JOIN (
	select [Environment]
			,[Date] 
			,[AccountCode]
			,[ContractEventTypeCode]
			,[AccountantOrLinked]
			,[PackageCode]
			,[NumberOfUsers]
			,[NumberOfAdministrations]
			,[MRR]
			,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
		as DateNumber
	from [domain].[ActivityDaily_Contracts]
	where Date <= @EndDate
)AS Contracts

ON Accounts.[Environment] = Contracts.[Environment] 
AND Accounts.[AccountCode] = Contracts.[AccountCode] 

LEFT JOIN (
		SELECT
			[Environment]
			,[LastContractUpgradeDate]
			,[AccountCode]
			,[ContractEventTypeCode]
			,[PackageCode]
			,[DateNumber]
		FROM
			ORDERED
		WHERE
			DateNumber = 1
	
)AS Contracts1

ON Accounts.[Environment] = Contracts1.[Environment] 
AND Accounts.[AccountCode] = Contracts1.[AccountCode] 

JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

JOIN [domain].[AccountsContract_Summary] AS Summary
	ON Contracts.[Environment] = Summary.[Environment] 
	AND Contracts.[AccountCode] = Summary.[AccountCode]	

WHERE Contracts.DateNumber = 1
AND PackageClassification.[Product] = @Product
AND Summary.[HadCommContract] = 1
AND Summary.[FirstCommStartDate] < @EndDate
AND (Summary.[FullCancellationRequestDate] > @EndDate OR Summary.[FullCancellationRequestDate] IS NULL)
AND Summary.[LatestCommFinalDate] > @EndDate
AND Accounts.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND Accounts.[AccountClassificationCode] in ('EOL', 'ACC', 'AC1', 'AC7', 'AC8', 'JBO', 'EO1')
