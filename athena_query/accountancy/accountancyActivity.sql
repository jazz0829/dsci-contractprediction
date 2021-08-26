SELECT Rtrim(activitydaily.accountcode) AS AccountCode,
         Rtrim(activitydaily.environment) AS Environment,
         continuousmonitoring,
         scanning,
         processmanagement
FROM activitydaily AS ActivityDaily
LEFT JOIN 
    (SELECT environment -- ok 
     , accountcode -- ok 
     , packagecode -- ok 
     , Row_number()
        OVER ( PARTITION BY environment, accountcode
    ORDER BY  DATE DESC) -- ok 
     AS DateNumber -- ok 
    
    FROM activitydaily_contracts -- ok 
    
    WHERE DATE <= DATE '2018-12-31' -- ok 
     )AS Contracts -- ok 
    ON Ltrim(Rtrim(Upper(activitydaily.environment))) = Ltrim(Rtrim(Upper(Contracts.environment)))
        AND activitydaily.accountcode = Contracts.accountcode
JOIN packageclassification AS PackageClassification
    ON Ltrim(Rtrim(Upper(Contracts.environment))) = Ltrim( Rtrim(Upper(packageclassification.environment)))
        AND Ltrim(Rtrim(Upper(Contracts.packagecode))) = Ltrim( Rtrim(Upper(packageclassification.packagecode)))
        
-- JOIN Continous Monitoring 
LEFT JOIN 
    (SELECT accountcode,
         environment,
         SUM(quantity) AS ContinuousMonitoring -- ok 
    
    FROM activitydaily AS ActivityDaily -- ok 
    
    WHERE activitydaily.activityid IN( 5017, 5018, 5019 ) -- ok 
    
            AND activitydaily.DATE >= DATE '2018-12-20' -- ok 
    
            AND activitydaily.DATE <= DATE '2018-12-31' -- ok 
    
    GROUP BY  accountcode, environment -- ok 
     ) CM
    ON CM.accountcode = activitydaily.accountcode
        AND Ltrim(Rtrim(Upper(CM.environment))) = Ltrim(Rtrim(Upper( activitydaily.environment))) 
-- JOIN Scan Service 
LEFT JOIN 
    (SELECT accountcode,
         environment,
         SUM(quantity) AS Scanning -- ok 
    
    FROM activitydaily AS ActivityDaily -- ok 
    
    WHERE activitydaily.activityid IN ( 5003, 5005 ) -- ok 
    
            AND activitydaily.DATE >= DATE '2018-12-20' -- ok 
    
            AND activitydaily.DATE <= DATE '2018-12-31' -- ok 
    
    GROUP BY  accountcode, environment -- ok 
     ) SR
    ON SR.accountcode = activitydaily.accountcode
        AND Ltrim(Rtrim(Upper(SR.environment))) = Ltrim(Rtrim(Upper( activitydaily.environment))) 
-- JOIN Process management 
LEFT JOIN 
    (SELECT accountcode,
         environment,
         SUM(quantity) AS ProcessManagement -- ok 
    
    FROM activitydaily AS ActivityDaily -- ok 
    
    WHERE activitydaily.activityid IN ( 5004 ) -- ok 
    
            AND activitydaily.DATE >= DATE '2018-12-20' -- ok 
    
            AND activitydaily.DATE <= DATE '2018-12-31' -- ok 
    
    GROUP BY  accountcode, environment -- ok 
     ) PM
    ON PM.accountcode = activitydaily.accountcode
        AND Ltrim(Rtrim(Upper(PM.environment))) = Ltrim(Rtrim(Upper( activitydaily.environment)))
WHERE Ltrim(Rtrim(Upper(activitydaily.environment))) IN ( 'NL' )
        AND activitydaily.DATE >= DATE '2018-12-20'
        AND activitydaily.DATE <= DATE '2018-12-31'
        AND Contracts.datenumber = 1
        AND packageclassification.product = 'Accountancy'
GROUP BY  activitydaily.accountcode, activitydaily.environment, continuousmonitoring, scanning, processmanagement
ORDER BY  accountcode 
