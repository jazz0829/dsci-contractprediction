DECLARE @EndDate DATE 
DECLARE @Window INT  
DECLARE @Environment VARCHAR(20)
DECLARE @Product VARCHAR(30)
DECLARE @StartDate DATE 

SET @EndDate = ?
SET @Window = ?
SET @Environment = ?
SET @Product = ?
SET @StartDate = DATEADD(DAY, -@Window, @EndDate)

SELECT RTRIM(Environment) AS Environment
	 , RTRIM(AccountCode) AS AccountCode
	 , MAX(EntrepreneurDaysActive) AS EntrepreneurDaysActive
	 , MAX(AccountantDaysActive) AS AccountantDaysActive
FROM (

SELECT
	Environment
	, AccountCode
	, CASE WHEN [USER] = 'Entrepreneur' THEN COUNT(DISTINCT(Date)) END AS EntrepreneurDaysActive
	, CASE WHEN [USER] = 'Accountant' THEN COUNT(DISTINCT(Date)) END AS AccountantDaysActive
FROM		
	(	
		SELECT 
			AD.AccountID
			, AD.AccountCode
			, AD.Environment
			, AD.DivisionCode
			, CASE WHEN AD.AccountID = U.AccountID THEN 'Entrepreneur' 
					WHEN AD.AccountID <> U.AccountID AND UserAccount.IsAccountant = 'Accountant' THEN 'Accountant'
					ELSE 'Other' END AS [User]
			, AD.[Date] 
			, SUM(Quantity) AS Quantity
		FROM
			domain.ActivityDaily AD

		INNER JOIN domain.Users U
			ON AD.UserID = U.UserID

		INNER JOIN domain.Accounts UserAccount		-- Joins accountID of the User, rather than what appears in the ActivityLog
			ON U.AccountID = UserAccount.AccountID 

		WHERE
			AD.ActivityID = 1   -- Only takes the total number of page views
			AND Date>= @StartDate
			AND Date <= @EndDate
		GROUP BY
			AD.AccountID
			, AD.AccountCode
			, AD.Environment
			, AD.DivisionCode
			, AD.[Date]
			, U.AccountID
			, UserAccount.IsAccountant
	) SQ
WHERE Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
GROUP BY
	Environment
	, AccountCode
	, [User]
) SUB
GROUP BY 
Environment, AccountCode


