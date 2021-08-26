SELECT ISUS.AccountCode
           ,ISUS.Environment
           ,ISUS.Score
           ,PackageClassification.Product
           ,ISUS.Phase
           ,cast (ISUS.PredictDate AS DATE) AS PredictDate
           ,DG.DowngradeDate
from ISUsageScore AS ISUS

left join PackageClassification AS PackageClassification
ON  RTRIM(LTRIM(UPPER(ISUS.Environment))) =  RTRIM(LTRIM(UPPER(PackageClassification.Environment)))
AND ISUS.PackageCode = PackageClassification.PackageCode


left join (
  SELECT distinct  RTRIM(LTRIM(UPPER(NewContract.Environment))) AS Environment
      ,RTRIM(LTRIM(UPPER(NewContract.AccountCode))) AS AccountCode
      ,NewContract.EventDate AS DowngradeDate

  FROM Contracts AS NewContract
  left join Contracts AS OldContract
  on NewContract.AccountCode = OldContract.AccountCode
  AND RTRIM(LTRIM(UPPER(NewContract.Environment))) = RTRIM(LTRIM(UPPER(OldContract.Environment)))
  AND NewContract.EventDate = OldContract.EventDate
  AND NewContract.ItemType = OldContract.ItemType
  AND NewContract.EventType = OldContract.EventType

  left join PackageClassification AS newClass
  on RTRIM(LTRIM(UPPER(newClass.Environment))) = RTRIM(LTRIM(UPPER(NewContract.Environment)))
  AND newClass.PackageCode = NewContract.PackageCode

  left join PackageClassification AS oldClass
  on RTRIM(LTRIM(UPPER(oldClass.Environment))) = RTRIM(LTRIM(UPPER(OldContract.Environment)))
  AND oldClass.PackageCode = OldContract.PackageCode

  WHERE NewContract.Environment = 'NL'
  AND NewContract.EventType = 'CDN'
  AND NewContract.EventDate > (SELECT  cast(PredictDate AS DATE) AS PredictDate
        FROM ISUsageScore
        LEFT JOIN PackageClassification AS PackageClassification
        ON RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = RTRIM(LTRIM(UPPER(PackageClassification.Environment)))
        AND ISUsageScore.PackageCode = PackageClassification.PackageCode
        WHERE Product = 'Wholesale Distribution'
        AND PredictDate <= DATE '2019-10-11'
        AND RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = 'NL'
        order by PredictDate desc
        limit 1)
  AND NewContract.EventDate < (SELECT  cast(PredictDate AS DATE) AS PredictDate
        FROM ISUsageScore
        LEFT JOIN PackageClassification AS PackageClassification
        ON RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = RTRIM(LTRIM(UPPER(PackageClassification.Environment)))
        AND ISUsageScore.PackageCode = PackageClassification.PackageCode
        WHERE Product = 'Wholesale Distribution'
        AND PredictDate <= DATE '2019-10-11'
        AND RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = 'NL'
        order by PredictDate desc
        limit 1) + interval '3' month  

  AND NewContract.ItemType = 'Package'
  AND NewContract.InflowOutflow = 'Inflow'
  AND OldContract.InflowOutflow = 'Outflow'
  AND oldClass.Product = 'Wholesale Distribution'
  AND newClass.Product = 'Accounting' ) AS DG
  
  on ISUS.AccountCode = CAST(DG.AccountCode AS Integer)
  AND RTRIM(LTRIM(UPPER(ISUS.Environment))) = RTRIM(LTRIM(UPPER(DG.Environment)))
  
  WHERE CAST(ISUS.PredictDate AS DATE) = (SELECT cast(PredictDate AS DATE) AS PredictDate
        FROM ISUsageScore
        LEFT JOIN PackageClassification AS PackageClassification
        ON RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = RTRIM(LTRIM(UPPER(PackageClassification.Environment)))
        AND ISUsageScore.PackageCode = PackageClassification.PackageCode
        WHERE Product = 'Wholesale Distribution'
        AND PredictDate <= DATE '2019-10-11'
        AND RTRIM(LTRIM(UPPER(ISUsageScore.Environment))) = 'NL'
        order by PredictDate desc
        limit 1)
   AND Product = 'Wholesale Distribution'

