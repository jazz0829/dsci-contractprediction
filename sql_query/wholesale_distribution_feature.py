import pandas as pd
from function.read_utils import read_sqlfile_to_df
from function import transform
import os, logging

class WholesaleDistributionFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.logger = logging.getLogger(__name__)

    def get_ds_wd(self):
        print('Getting Division Statistics Wholesale distribution...')
        self.logger.info('Getting Division Statistics Wholesale distribution...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        ds_wd = read_sqlfile_to_df(self.filepath + 'sql_query/wholesaleDistributionDivisionStatistics.sql', params)
        if len(ds_wd) > 0:
            data = pd.merge(data, ds_wd, how = 'left', on = ['AccountCode', 'Environment'])        
            int_cols = ["QuoteCount", "CustomerCount", "SupplierCount", "SalesInvoiceCount", 
                        "StockCountEntryCount","SalesOrderEntryCount", "PurchaseOrderEntryCount", 
                        "DeliveryEntryCount", "ItemCount", "SalesPriceListCount"]
            data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy())
        self.features = data
    
    def get_active_users_divs(self):
        print('Getting active users and divisions...')
        self.logger.info('Getting active users and divisions...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        user_div = read_sqlfile_to_df(self.filepath + 'sql_query/activeUsersDivisions.sql', params)
        if len(user_div) > 0:
            data = pd.merge(data, user_div, how = 'left', on = ['AccountCode', 'Environment']) 
            data['ActiveUsers'] = data['ActiveUsers'].fillna(0)
            data['ActiveDivisions'] = data['ActiveDivisions'].fillna(0)
        self.features = data
    
    def get_wholesale_distribution_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()
        # active days
        cf_obj.get_active_days()       
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()    
        cf_obj.get_mobile()
        cf_obj.get_label()
        self.features = cf_obj.features
        self.features['Sector'] = 'NonG'
        self.features.loc[self.features.SectorCode == 'G', 'Sector'] = 'G'
        self.features.loc[self.features.SectorCode.isnull(), 'Sector'] = 'Unknown'
        self.features['AccountName'] = self.features['AccountName'].str.lower()
        self.features['HasWholesaleName'] = self.features['AccountName'].str.contains("handel|trad|internation|product|materieel|wholesale")
        
        # division statistics wholesale
        self.get_ds_wd()
        self.get_active_users_divs()
                
        return self.features
        
    