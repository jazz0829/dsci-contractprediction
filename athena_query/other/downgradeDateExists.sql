SELECT *
FROM ISUsageScore as ISUsageScore

LEFT JOIN PackageClassification AS PackageClassification
ON LTRIM(RTRIM(UPPER(ISUsageScore.Environment))) = LTRIM(RTRIM(UPPER(PackageClassification.Environment)))
AND LTRIM(RTRIM(UPPER(ISUsageScore.PackageCode))) = LTRIM(RTRIM(UPPER(PackageClassification.PackageCode)))

where LTRIM(RTRIM(PackageClassification.Product)) = 'Wholesale Distribution'
AND ISUsageScore.Environment = 'NL'
AND CAST(ISUsageScore.ReferenceDate as DATE) = date '2018-10-13'
AND ISUsageScore.Phase = 'All'
ORDER BY score desc
LIMIT 1


