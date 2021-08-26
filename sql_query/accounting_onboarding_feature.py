from function.read_utils import read_sqlfile_to_df
import pandas as pd
from function import transform
import os, logging

class AccountingOnboardingFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.logger = logging.getLogger(__name__)

    def get_masterdata_setup_type(self):
        print('Getting masterdata setup type...')
        self.logger.info('Getting accountancy activities...')
        data = self.features.copy()
        params = [self.reference_date, self.config['country'], self.config['product']]
        masterdata = read_sqlfile_to_df(self.filepath + 'sql_query/masterDataSetupType.sql', params)
        if len(masterdata) > 0:
            data = pd.merge(data, masterdata, how = 'left', on = ['AccountCode', 'Environment']) 
            data['MasterDataSetupType'] = data['MasterDataSetupType'].fillna('Unknown')
        self.features = data
        
    def remove_default_entries(self):
        col = ["BankEntryCount", "CashEntryCount", "SalesEntryCount", "PurchaseEntryCount",
                 "GeneralJournalEntryCount", "AccountCount"]
        default_nl = [18, 2, 56, 88, 145, 25]
        default_be = [3, 1, 25, 25, 6, 7]
        data = self.features.copy()
        idx_nl = data.index[(data['TenureInMonths']< 3) & (data.BankEntryCount >= default_be[0]) & 
             (data.CashEntryCount >= default_be[1]) & (data.SalesEntryCount >= default_be[2]) & 
             (data.PurchaseEntryCount >= default_be[3]) & 
             (data.GeneralJournalEntryCount >= default_be[4]) & 
             (data.AccountCount >= default_be[5]) & (data.Environment == 'NL')].tolist()
        data.loc[idx_nl, col] = data.loc[idx_nl, col] - default_nl
        
        idx_be = data.index[(data['TenureInMonths']< 3) & (data.BankEntryCount >= default_be[0]) & 
             (data.CashEntryCount >= default_be[1]) & (data.SalesEntryCount >= default_be[2]) & 
             (data.PurchaseEntryCount >= default_be[3]) & 
             (data.GeneralJournalEntryCount >= default_be[4]) & 
             (data.AccountCount >= default_be[5]) & (data.Environment =='BE')].tolist()
        data.loc[idx_be, col] = data.loc[idx_be, col] - default_be
        int_cols = ["PurchaseEntryCount", "BankEntryCount", "SalesEntryCount",
                    "AccountCount", "CashEntryCount", "GeneralJournalEntryCount"]
        data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy()) # recaculate log transformation; can be refined
        self.features = data
        
        
    def get_accounting_onboarding_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()    
        cf_obj.features = cf_obj.features[cf_obj.features.TenureInMonths < 6]
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()  
        # active accountant
        cf_obj.get_active_accountant() 
        # pageviews
        cf_obj.get_pageviews()
        # label        
        cf_obj.get_label()
        
        self.features = cf_obj.features
        
        self.get_masterdata_setup_type()
        self.remove_default_entries()
        
        
                
        return self.features
        
    