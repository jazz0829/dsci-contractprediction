DECLARE @EndDate DATE 
DECLARE @Window INT  
DECLARE @Environment VARCHAR(20)
DECLARE @StartDate DATE 

SET @EndDate = ?
SET @Window = ?
SET @Environment = ?
SET @StartDate = DATEADD(DAY, -@Window, @EndDate)

Select 	
	RTRIM(A.AccountCode) AS AccountCode,
	RTRIM(A.Environment) AS Environment,
	count(distinct(Date)) as ActiveDaysOnLinkedCompanies,
	count(Distinct AD.DivisionCode)AS [NumActiveLinkedCompanies],
	SUM(Quantity) AS [PageviewsOnLinkedCompanies]
FROM domain.Accounts A

JOIN domain.Users U
ON A.AccountID=U.AccountID

JOIN domain.ActivityDaily AD
ON AD.UserID= CAST(U.UserID AS varchar(36))

WHERE 
	A.AccountID<>AD.AccountID
	AND Date Between @StartDate and @EndDate
	AND ActivityID='1'
	AND A.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )

GROUP BY 
	A.AccountCode,
	A.Environment
