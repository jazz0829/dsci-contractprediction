DECLARE @PastDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @Phase VARCHAR(20)

SET @PastDate = ?
SET @Environment = ?
SET @Product = ?
SET @Phase = ?

SELECT HS.AccountCode
	   ,HS.Environment
	   ,HS.HealthScore
	   ,PackageClassification.Product
	   ,HS.Phase
	   ,cast (HS.PredictDate as date) as PredictDate
	   ,ACS.FullCancellationRequestDate
	   ,ACS.LatestCommFinalDate
from [CustomerIntelligence].[publish].[HealthScore] as HS

left join [domain].[PackageClassification] AS PackageClassification
ON HS.[Environment] = PackageClassification.[Environment] 
AND HS.[PackageCode] = PackageClassification.[PackageCode]

left join [CustomerIntelligence].[domain].[AccountsContract_Summary] as ACS
on HS.AccountCode = ACS.AccountCode
and HS.Environment = ACS.Environment

WHERE cast(HS.PredictDate as date) = (
	SELECT top 1 cast(PredictDate as date) as PredictDate
	FROM [CustomerIntelligence].[publish].[HealthScore]
	LEFT JOIN [domain].[PackageClassification] AS PackageClassification
	ON HealthScore.[Environment] = PackageClassification.[Environment] 
	AND HealthScore.[PackageCode] = PackageClassification.[PackageCode]
	where Product = @Product
	and Phase = @Phase
	and PredictDate <= @PastDate
	and HealthScore.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
	order by PredictDate desc)

and Product = @Product
and Phase = @Phase


