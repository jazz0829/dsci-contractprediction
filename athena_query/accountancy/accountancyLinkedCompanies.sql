SELECT RTRIM(A.AccountCode) AS AccountCode,
         RTRIM(A.Environment) AS Environment,
         count(distinct(Date)) AS ActiveDaysOnLinkedCompanies,
         count(Distinct AD.DivisionCode)AS NumActiveLinkedCompanies,
         SUM(Quantity) AS PageviewsOnLinkedCompanies
FROM Accounts A

JOIN Users U
    ON LTRIM(RTRIM(UPPER(A.AccountID))) = LTRIM(RTRIM(UPPER(U.AccountID)))
    
JOIN ActivityDaily AD
    ON AD.UserID= CAST(U.UserID AS varchar(36))
    
WHERE LTRIM(RTRIM(UPPER(A.AccountID))) <> LTRIM(RTRIM(UPPER(AD.AccountID)))
        AND Date >= date '2018-12-31'
        AND Date <= date '2019-11-01'
        AND ActivityID=1
        AND LTRIM(RTRIM(UPPER(A.Environment))) IN ( 'NL' )
        
GROUP BY  A.AccountCode, A.Environment
