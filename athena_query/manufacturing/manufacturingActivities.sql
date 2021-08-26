Select RTRIM(LTRIM(UPPER(ActivityDaily.AccountCode))) AccountCode
	 , RTRIM(LTRIM(UPPER(ActivityDaily.Environment))) Environment
	 , ManufacturingBasic
	 , ManufacturingAdvanced


From ActivityDaily as ActivityDaily

LEFT JOIN (
select RTRIM(LTRIM(UPPER(Environment))) as Environment
		,RTRIM(LTRIM(UPPER(AccountCode))) AS AccountCode
		,RTRIM(LTRIM(UPPER(PackageCode))) AS PackageCode
		,row_number() over (partition by Environment, AccountCode order by Date desc)
	as DateNumber
from ActivityDaily_Contracts
where Date <= '2019-5-11'
)AS Contracts

ON RTRIM(LTRIM(UPPER(ActivityDaily.Environment))) = RTRIM(LTRIM(UPPER(Contracts.Environment)))
AND RTRIM(LTRIM(UPPER(ActivityDaily.AccountCode))) = RTRIM(LTRIM(UPPER(Contracts.AccountCode))) 

JOIN PackageClassification AS PackageClassification
ON RTRIM(LTRIM(UPPER(Contracts.Environment))) = RTRIM(LTRIM(UPPER(PackageClassification.Environment)))
AND RTRIM(LTRIM(UPPER(Contracts.PackageCode))) = RTRIM(LTRIM(UPPER(PackageClassification.PackageCode)))

-- JOIN BASIC QUANTITY
LEFT JOIN 
(SELECT 
	RTRIM(LTRIM(UPPER(AccountCode))) as AccountCode, 
	RTRIM(LTRIM(UPPER(Environment))) as Environment,
	SUM(Quantity) as ManufacturingBasic
From ActivityDaily as ActivityDaily

Where (RTRIM(LTRIM(UPPER(ActivityDaily.ActivityID))) in (5175, 5060, 5061, 5062, 5058, 5173, 5261, 5259))
and ActivityDaily.Date >= '2018-12-20'
and ActivityDaily.Date <= '2019-5-11'
Group by RTRIM(LTRIM(UPPER(AccountCode))), RTRIM(LTRIM(UPPER(Environment)))
) BAS
ON RTRIM(LTRIM(UPPER(BAS.AccountCode)))=RTRIM(LTRIM(UPPER(ActivityDaily.AccountCode))) 
AND RTRIM(LTRIM(UPPER(BAS.Environment)))=RTRIM(LTRIM(UPPER(ActivityDaily.Environment)))

-- JOIN ADVANCED QUANTITY
LEFT JOIN 
(SELECT RTRIM(LTRIM(UPPER(AccountCode))), 
	RTRIM(LTRIM(UPPER(Environment)))
	, SUM(Quantity) as ManufacturingAdvanced
From ActivityDaily as ActivityDaily 

Where  (RTRIM(LTRIM(UPPER(ActivityDaily.ActivityID))) in (5044, 5045, 5046, 5056, 5168))
and ActivityDaily.Date >= '2018-12-20'
and ActivityDaily.Date <= '2019-5-11'
Group by RTRIM(LTRIM(UPPER(AccountCode))), RTRIM(LTRIM(UPPER(Environment)))

) ADV
ON RTRIM(LTRIM(UPPER(ADV.AccountCode)))=RTRIM(LTRIM(UPPER(ActivityDaily.AccountCode)))
AND RTRIM(LTRIM(UPPER(ADV.Environment)))=RTRIM(LTRIM(UPPER(ActivityDaily.Environment)))



Where RTRIM(LTRIM(UPPER(ActivityDaily.Environment))) = 'NL'
and ActivityDaily.Date >= '2018-12-20'
and ActivityDaily.Date <= '2019-5-11'
AND Contracts.DateNumber = 1
AND PackageClassification.Product = 'Manufacturing'

Group by RTRIM(LTRIM(UPPER(ActivityDaily.AccountCode))),
	RTRIM(LTRIM(UPPER(ActivityDaily.Environment))), 
	RTRIM(LTRIM(UPPER(ManufacturingBasic))), 
	RTRIM(LTRIM(UPPER(ManufacturingAdvanced)))
order by AccountCode


