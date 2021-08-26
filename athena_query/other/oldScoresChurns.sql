SELECT HS.AccountCode
	   ,HS.Environment
	   ,HS.HealthScore
	   ,PackageClassification.Product
	   ,HS.Phase
	   ,cast (HS.PredictDate as date) as PredictDate
	   ,ACS.FullCancellationRequestDate
	   ,ACS.LatestCommFinalDate
from publish_HealthScore as HS

left join PackageClassification AS PackageClassification
ON LTRIM(RTRIM(UPPER(HS.Environment))) =  LTRIM(RTRIM(UPPER(PackageClassification.Environment)))
AND  LTRIM(RTRIM(UPPER(HS.PackageCode))) =  LTRIM(RTRIM(UPPER(PackageClassification.PackageCode)))

left join AccountsContract_Summary as ACS
on LTRIM(RTRIM(UPPER(HS.AccountCode))) = LTRIM(RTRIM(UPPER(ACS.AccountCode))) 
and LTRIM(RTRIM(UPPER(HS.Environment))) = LTRIM(RTRIM(UPPER(ACS.Environment))) 

WHERE cast(HS.PredictDate as date) = (
	SELECT  cast(PredictDate as date) as PredictDate
	FROM publish_HealthScore
	LEFT JOIN PackageClassification AS PackageClassification
	ON LTRIM(RTRIM(UPPER(HealthScore.Environment))) = LTRIM(RTRIM(UPPER(PackageClassification.Environment)))
	AND LTRIM(RTRIM(UPPER(HealthScore.PackageCode))) = LTRIM(RTRIM(UPPER(PackageClassification.PackageCode)))
	where Product = "Accountancy"
	and Phase = "All"
	and PredictDate <= date '' 
	and LTRIM(RTRIM(UPPER(HealthScore.Environment))) = 'NL' 
	order by PredictDate desc
	LIMIT 1)

and Product = "Accountancy"
and Phase = "All"


