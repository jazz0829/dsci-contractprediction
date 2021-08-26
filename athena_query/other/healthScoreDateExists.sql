SELECT * FROM 
 publish_HealthScore as HealthScore

LEFT JOIN PackageClassification AS PackageClassification
ON LTRIM(RTRIM(UPPER(HealthScore.Environment))) = LTRIM(RTRIM(UPPER(PackageClassification.Environment)))
AND LTRIM(RTRIM(UPPER(HealthScore.PackageCode))) = LTRIM(RTRIM(UPPER(PackageClassification.PackageCode)))

where PackageClassification.Product = 'Accountancy'
AND LTRIM(RTRIM(UPPER(HealthScore.Environment))) = 'NL'
AND CAST(DATE_PARSE(HealthScore.ReferenceDate, '%m-%d-%y %h:%i %p') as DATE) = date '2019-10-24'
AND HealthScore.Phase = 'All'

ORDER BY healthscore desc
LIMIT 1
