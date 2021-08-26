import pandas as pd
from function.read_utils import read_sqlfile_to_df
from function import transform
import os, logging

class AccoutancyFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.logger = logging.getLogger(__name__)

    def get_accountancy_linked_companies(self):
        print('Getting accountancy linked companies...')
        self.logger.info('Getting accountancy linked companies...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country']]
        ds_wd = read_sqlfile_to_df(self.filepath + 'sql_query/accountancyLinkedCompanies.sql', params)
        if len(ds_wd) > 0:
            data = pd.merge(data, ds_wd, how = 'left', on = ['AccountCode', 'Environment'])        
            int_cols = ["NumActiveLinkedCompanies", "PageviewsOnLinkedCompanies"]
            data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy())
        self.features = data
    
    def get_accountancy_activities(self):
        print('Getting accountancy activities...')
        self.logger.info('Getting accountancy activities...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        user_div = read_sqlfile_to_df(self.filepath + 'sql_query/accountancyActivity.sql', params)
        if len(user_div) > 0:
            data = pd.merge(data, user_div, how = 'left', on = ['AccountCode', 'Environment']) 
            act_cols = ['ContinuousMonitoring','Scanning', 'ProcessManagement']
            data.loc[:, act_cols] = transform.to_zero(data[act_cols].copy())
            data['OtherAccoutancyActivities'] = data[act_cols].sum(axis=1)          
            df_act = data['OtherAccoutancyActivities'].to_frame(name='OtherAccoutancyActivities')                                   
            data['LogOtherAccoutancyActivities'] = transform.log1p_columns(df_act.copy())
        self.features = data
    
    def get_accountancy_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()
        # exclude basic
        cf_obj.features = cf_obj.features[cf_obj.features.Edition != 'Basic']
        # active days
        cf_obj.get_active_days()       
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()
        # lable
        cf_obj.get_label()
        self.features = cf_obj.features
        self.features['SalesInvoice'] = self.features['SalesInvoice'].fillna(0)
        self.features['ProjectTotalTimeEntries'] = self.features['ProjectTotalTimeEntries'].fillna(0)
        sum_df = (self.features.SalesInvoice  + self.features.ProjectTotalTimeEntries).to_frame('TimeSalesInvoiceEntry')
        self.features['LogTimeSalesInvoiceEntry'] = transform.log1p_columns(sum_df.copy())
        # accoutancy activities
        self.get_accountancy_activities()
        # linked companies
        self.get_accountancy_linked_companies()
                
        return self.features
        
    