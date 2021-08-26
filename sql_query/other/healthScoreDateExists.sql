DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @Phase VARCHAR(20)

SET @EndDate = ?
SET @Environment = ?
SET @Product = ?
SET @Phase = ?


SELECT TOP 1 *
FROM [publish].[HealthScore] as HealthScore

LEFT JOIN [domain].[PackageClassification] AS PackageClassification
ON HealthScore.[Environment] = PackageClassification.[Environment] 
AND HealthScore.[PackageCode] = PackageClassification.[PackageCode]

where PackageClassification.[Product] = @Product
AND HealthScore.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND HealthScore.[ReferenceDate] = @EndDate
AND HealthScore.[Phase] = @Phase

