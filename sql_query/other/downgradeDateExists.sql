DECLARE @EndDate DATE 
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @Phase VARCHAR(20)

SET @EndDate = ?
SET @Environment = ?
SET @Product = ?
SET @Phase = ?


SELECT TOP 1 *
FROM [publish].[ISUsageScore] as ISUsageScore

LEFT JOIN [domain].[PackageClassification] AS PackageClassification
ON ISUsageScore.[Environment] = PackageClassification.[Environment] 
AND ISUsageScore.[PackageCode] = PackageClassification.[PackageCode]

where PackageClassification.[Product] = @Product
AND ISUsageScore.[Environment] in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
AND ISUsageScore.[ReferenceDate] = @EndDate
AND ISUsageScore.[Phase] = @Phase

