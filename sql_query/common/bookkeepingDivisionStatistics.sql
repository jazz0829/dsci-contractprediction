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


SELECT
       RTRIM(Contracts.[AccountCode]) as AccountCode
      ,RTRIM(Contracts.[Environment]) as Environment
	   ,count( distinct SQ.DivisionCode) as NumActivatedDivisions
       
       , MAX(
                     CASE 
                     WHEN SQ.ABNAMROBankAccounts > 0 THEN 1
                     WHEN SQ.INGBankAccounts > 0 THEN 1
                     WHEN SQ.RabobankAccounts > 0 THEN 1
                     ELSE 0 
                     END
              ) AS Big3BankAccount                                              -- If the customer has at least one bank account for ABNAMRO, ING, or Rabobank in any division then this is set to 1, otherwise 0
       , MAX(SQ.AutomaticBankLink) AS BankLink                    -- If the customer has a bank link established for any of their divisions then this is set to 1, else 0
       , MAX(
                     CASE 
                     WHEN DSS.SalesInvoiceFirstDate <= @EndDate THEN 1
                     ELSE 0
                     END
              ) AS EverSentInvoice                                              -- If the customer has finalized an invoice from at least one of their divisions that occurred on or before the date set in the variable then this is set to 1, else 0
        , MAX(SQ.CurrencyCount) AS CurrencyCount
        , SUM(SumGLTransactionsCount) AS GLTransactions
        , SUM(SumSalesInvoiceCount) AS SalesInvoice
        , SUM(SumPurchaseEntryCount) AS PurchaseEntryCount
		  , SUM(SumStockCountEntryCount) AS StockCountEntry
		  , SUM(SumBankEntryCount) AS BankEntryCount
		  , SUM(SumCashEntryCount) AS CashEntryCount
		  , SUM(SumSalesEntryCount) AS SalesEntryCount
		  , SUM(SumGeneralJournalEntryCount) AS GeneralJournalEntryCount
		  , SUM(SumAccountCount) AS AccountCount 
		  , SUM(SumProjectTotalTimeEntries) AS ProjectTotalTimeEntries
FROM
       (
              SELECT  
                     DSD.Environment
                     , DSD.DivisionCode
                     , D.AccountID
                     , DSD.[Date]
                     , DSD.ABNAMROBankAccounts
                     , DSD.INGBankAccounts
                     , DSD.RabobankAccounts
                     , DSD.AutomaticBankLink
                     , DSD.CurrencyCount
                     , ROW_NUMBER() OVER(PARTITION BY DSD.Environment, DSD.DivisionCode ORDER BY DSD.[Date] DESC) AS RN            -- Creates a row number in order to select the most recent divisionstatistics records before or on the date set in the variable
              FROM domain.DivisionStatistics_Daily DSD
              INNER JOIN domain.Divisions D
                     ON DSD.Environment = D.Environment
                     AND DSD.DivisionCode = D.DivisionCode
              WHERE
                     (D.Deleted IS NULL OR D.Deleted >= @EndDate)           -- Only includes divisions that were not deleted or only deleted after the date set to the variable
                     AND DSD.[Date] <= @EndDate                                    -- Only includes divisionstatistics records for the date on or before the date set in the variable
                     AND DSD.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )                                                    -- Only NL
					 AND D.BlockingStatusCode < 100      
       ) SQ
INNER JOIN domain.DivisionStatistics_Summary DSS
       ON DSS.Environment = SQ.Environment
       AND DSS.DivisionCode = SQ.DivisionCode

LEFT JOIN 
       (
             SELECT 
                    Environment
                    , DivisionCode
                    , SUM(GLTransactionsCount) AS SumGLTransactionsCount
                    , SUM(BankTransactions) AS SumBankTransactions
					, SUM(BankEntryCount) AS SumBankEntryCount
					, SUM(CashEntryCount) AS SumCashEntryCount
                    , SUM(SalesInvoiceCount) AS SumSalesInvoiceCount
					, SUM(PurchaseEntryCount) AS SumPurchaseEntryCount
                    , SUM(StockCountEntryCount) AS SumStockCountEntryCount
					, SUM(SalesEntryCount) AS SumSalesEntryCount
					, SUM(GeneralJournalEntryCount) AS SumGeneralJournalEntryCount
					, SUM(AccountCount) AS SumAccountCount
					, SUM(ProjectTotalTimeEntries) AS SumProjectTotalTimeEntries
             FROM domain.DivisionStatistics_DailyChanges 
             WHERE
                    Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
                    AND [Date] BETWEEN @StartDate AND @EndDate
             GROUP BY
                    Environment
                    , DivisionCode
       ) DSDC
       ON SQ.Environment = DSDC.Environment
       AND SQ.DivisionCode = DSDC.DivisionCode


LEFT JOIN (
select [Environment]
              ,[AccountCode]
              ,[PackageCode]
              ,[AccountID]
              ,row_number() over (partition by [Environment], [AccountCode] order by Date desc)
       as DateNumber
from [domain].[ActivityDaily_Contracts]
where Date <= @EndDate
)AS Contracts

ON SQ.[AccountID] = Contracts.[AccountID] 


LEFT  JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE RN = 1                                    -- Selects only the latest record on or before date set to variable (based on row_number query)
AND PackageClassification.[Product] = @Product
AND Contracts.[DateNumber] = 1
GROUP BY 
       SQ.AccountID
       ,Contracts.[AccountCode]
       ,Contracts.[Environment]
