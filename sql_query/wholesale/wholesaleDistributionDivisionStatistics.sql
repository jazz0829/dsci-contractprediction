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
    , SUM(LastRec.QuoteCount - FirstRec.QuoteCount) AS QuoteCount
    , SUM(LastRec.CustomerCount - FirstRec.CustomerCount) AS CustomerCount
    , SUM(LastRec.SupplierCount - FirstRec.SupplierCount) AS SupplierCount
    , SUM(LastRec.SalesInvoiceCount - FirstRec.SalesInvoiceCount) AS SalesInvoiceCount 
	, SUM(LastRec.StockCountEntryCount - FirstRec.StockCountEntryCount) AS StockCountEntryCount
	, SUM(LastRec.SalesOrderEntryCount - FirstRec.SalesOrderEntryCount) AS SalesOrderEntryCount
	, SUM(LastRec.PurchaseOrderEntryCount - FirstRec.PurchaseOrderEntryCount) AS PurchaseOrderEntryCount
	, SUM(LastRec.DeliveryEntryCount - FirstRec.DeliveryEntryCount) AS DeliveryEntryCount
	, SUM(LastRec.ItemCount - FirstRec.ItemCount) AS ItemCount
	, SUM(LastRec.SalesPriceListCount - FirstRec.SalesPriceListCount) AS SalesPriceListCount
FROM
    (
            SELECT 
                    Environment
                    , DivisionCode
                    , [Date]
                    , ISNULL(QuoteCount, 0) AS QuoteCount
                    , ISNULL(CustomerCount, 0) AS CustomerCount
                    , ISNULL(SupplierCount, 0) AS SupplierCount
                    , ISNULL(SalesInvoiceCount, 0) AS SalesInvoiceCount
					, ISNULL(StockCountEntryCount, 0) AS StockCountEntryCount	
					, ISNULL(SalesOrderEntryCount, 0) AS SalesOrderEntryCount			
					, ISNULL(PurchaseOrderEntryCount, 0) AS PurchaseOrderEntryCount
					, ISNULL(DeliveryEntryCount, 0) AS DeliveryEntryCount	
					, ISNULL(ItemCount, 0) AS ItemCount
					, ISNULL(SalesPriceListCount, 0) AS SalesPriceListCount
                    , ROW_NUMBER() OVER (PARTITION BY Environment, DivisionCode ORDER BY Date) AS RN_First
            FROM domain.DivisionStatistics_Daily 
            WHERE 
                    Date >= @StartDate   --NEED TO MAKE DATES DYNAMIC
                    AND Date <= @EndDate  --NEED TO MAKE DATES DYNAMIC
    ) FirstRec
INNER JOIN 
    (
            SELECT 
                    Environment
                    , DivisionCode
                    , [Date]
                    , ISNULL(QuoteCount, 0) AS QuoteCount
                    , ISNULL(CustomerCount, 0) AS CustomerCount
                    , ISNULL(SupplierCount, 0) AS SupplierCount
                    , ISNULL(SalesInvoiceCount, 0) AS SalesInvoiceCount
					, ISNULL(StockCountEntryCount, 0) AS StockCountEntryCount
					, ISNULL(SalesOrderEntryCount, 0) AS SalesOrderEntryCount
					, ISNULL(PurchaseOrderEntryCount, 0) AS PurchaseOrderEntryCount
					, ISNULL(DeliveryEntryCount, 0) AS DeliveryEntryCount
					, ISNULL(ItemCount, 0) AS ItemCount
					, ISNULL(SalesPriceListCount, 0) AS SalesPriceListCount
                    , ROW_NUMBER() OVER (PARTITION BY Environment, DivisionCode ORDER BY Date DESC) AS RN_Last
            FROM domain.DivisionStatistics_Daily 
            WHERE 
                    Date >= @StartDate  --NEED TO MAKE DATES DYNAMIC
                    AND Date <= @EndDate  --NEED TO MAKE DATES DYNAMIC
    ) LastRec
ON FirstRec.Environment = LastRec.Environment
AND FirstRec.DivisionCode = LastRec.DivisionCode
AND FirstRec.RN_First = 1
AND LastRec.RN_Last = 1

INNER JOIN domain.Divisions D
    ON D.Environment = FirstRec.Environment
    AND D.DivisionCode = FirstRec.DivisionCode


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

ON D.[AccountID] = Contracts.[AccountID] 

LEFT  JOIN [domain].[PackageClassification] AS PackageClassification
ON Contracts.[Environment] = PackageClassification.[Environment] 
AND Contracts.[PackageCode] = PackageClassification.[PackageCode]

WHERE PackageClassification.[Product] = @Product
AND Contracts.[DateNumber] = 1
and D.Environment in ( SELECT [value] FROM STRING_SPLIT(@Environment, ',') )
and (D.Deleted IS NULL OR D.Deleted > @EndDate)           
AND D.BlockingStatusCode < 100  
             
GROUP BY
   Contracts.AccountCode
   ,Contracts.[Environment]