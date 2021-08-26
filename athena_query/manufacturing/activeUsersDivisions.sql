SELECT  LTRIM(RTRIM(UPPER(Activity.Environment))) as Environment      
      ,LTRIM(RTRIM(UPPER(Activity.AccountCode))) as AccountCode
	  ,count(distinct Activity.UserID) AS ActiveUsers
	  ,count(distinct Activity.DivisionCode) As ActiveDivisions

FROM  ActivityDaily AS Activity
  
LEFT JOIN (
select LTRIM(RTRIM(UPPER(Environment))) as Environment
		,LTRIM(RTRIM(UPPER(AccountCode))) as accountcode
		,LTRIM(RTRIM(UPPER(PackageCode))) as packagecode
		,row_number() over (partition by Environment, AccountCode order by Date desc)
	as DateNumber
from ActivityDaily_Contracts
where Date <= DATE '2019-5-11'
)AS Contracts

   ON LTRIM(RTRIM(UPPER(Activity.Environment))) = LTRIM(RTRIM(UPPER(Contracts.Environment)))
   AND LTRIM(RTRIM(UPPER(Activity.AccountCode))) = LTRIM(RTRIM(UPPER(Contracts.AccountCode)))

LEFT  JOIN PackageClassification AS PackageClassification
   ON LTRIM(RTRIM(UPPER(Contracts.Environment))) = LTRIM(RTRIM(UPPER(PackageClassification.Environment)))
   AND LTRIM(RTRIM(UPPER(Contracts.PackageCode))) = LTRIM(RTRIM(UPPER(PackageClassification.PackageCode)))

WHERE Activity.ActivityID = 1
AND LTRIM(RTRIM(UPPER(Activity.Environment))) in ( 'NL' )
AND Activity.Date between DATE '2018-12-20' and  DATE '2019-5-11'

AND Contracts.DateNumber = 1
AND PackageClassification.Product = 'Accountancy'

GROUP BY  LTRIM(RTRIM(UPPER(Activity.Environment))) 
		,LTRIM(RTRIM(UPPER(Activity.AccountCode)))
