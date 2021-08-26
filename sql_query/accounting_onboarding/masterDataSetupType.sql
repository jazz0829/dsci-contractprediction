DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)

SET @EndDate = ?
SET @Environment = ?
SET @Product = ?


SELECT 
	RTRIM(Contracts.Environment) AS Environment,
	RTRIM(AccountCode) AS AccountCode,
	Category as MasterDataSetupType
FROM(
			Select 	Environment,
					DivisionCode,
					AccountID,
					DivisionCreated,
					MasterDataSetupType,
					CategoryRank,
					Category,
					MasterDataSetupStatus,
					EventStartTime,
					ROW_NUMBER() OVER (Partition BY AccountID ORDER BY SUB.CategoryRank asc, DivisionCreated asc) AS RowNumber
			FROM(
	
				Select 
					DMDS.Environment,
					DMDS.DivisionCode,
					DMDS.AccountID,
					DivisionCreated,
					MasterDataSetupType,
					CASE
						WHEN (MasterDataSetupType='Conversion' OR MasterDataSetupType='XML/CSV_Import') THEN '1'
						WHEN (MasterDataSetupType='AccountantTemplate' OR MasterDataSetupType='DivisionTransfer') THEN '2'
						WHEN (MasterDataSetupType='ExactTemplate') THEN '3'
						WHEN (MasterDataSetupType='Demo') THEN '4'
						WHEN (MasterDataSetupType='DivisionCopy' OR MasterDataSetupType='Manual' OR MasterDataSetupType='Empty') THEN '5'
					End AS CategoryRank,
					CASE
						WHEN (MasterDataSetupType='Conversion' OR MasterDataSetupType='XML/CSV_Import') THEN 'Other Software'
						WHEN (MasterDataSetupType='AccountantTemplate' OR MasterDataSetupType='DivisionTransfer') THEN 'Accountant Related'
						WHEN (MasterDataSetupType='ExactTemplate') THEN 'Exact Template'
						WHEN (MasterDataSetupType='Demo') THEN 'Demo'
						WHEN (MasterDataSetupType='DivisionCopy' OR MasterDataSetupType='Manual' OR MasterDataSetupType='Empty') THEN 'Other'
					End AS Category,
					MasterDataSetupStatus,
					EventStartTime

				From domain.Divisions_MasterDataSetup DMDS
				JOIN domain.Divisions DIV
				ON DMDS.DivisionCode=DIV.DivisionCode  AND DMDS.Environment=DIV.Environment
				WHERE 
					MasterDataSetupType IS NOT NULL
					AND MasterDataSetupType <> 'TemporaryDivision'
				)SUB
)SUB1


LEFT JOIN (
select [AccountCode]
		,[Environment]
		,[AccountID]
		,[PackageCode]
		,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
	as DateNumber
from [domain].[ActivityDaily_Contracts]
where Date <= @EndDate
)AS Contracts

ON SUB1.AccountID = Contracts.AccountID

JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE SUB1.RowNumber='1'
and SUB1.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND Contracts.[DateNumber] = 1
AND PackageClassification.[Product] = @Product