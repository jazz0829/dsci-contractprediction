import pandas as pd
from function.read_utils import read_sqlfile_to_df
from function import timeframe
from function import transform
import os, logging

class CommonFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.end_date = reference_date + pd.DateOffset(months=self.config['period'])
        self.logger = logging.getLogger(__name__)

    def get_valid_days(self):
        self.features['valid_contract_days'] = timeframe.get_valid_comm_days(self.features, self.reference_date, 
               self.config['window_contract']) + timeframe.get_valid_trial_days(self.features, 
               self.reference_date, self.config['window_contract'])
        self.features['valid_activity_days']= timeframe.get_valid_comm_days(self.features, self.reference_date, 
              self.config['window_activities']) + timeframe.get_valid_trial_days(self.features, 
              self.reference_date, self.config['window_activities'])   

    def get_contract(self):    
        print('Getting contract...')
        self.logger.info('Getting contract...')
        params = [self.reference_date, self.config['country'], self.config['product']]
        data = read_sqlfile_to_df(self.filepath + 'sql_query/accountsContractDaily.sql', params)
        date_cols = ["FirstCommStartDate", "LatestCommFinalDate", "FirstTrialStartDate", 
                     "LatestTrialFinalDate", "FullCancellationRequestDate", 
                     "LastContractUpgradeDate"]
        
        data[date_cols] = data[date_cols].applymap(lambda x : pd.to_datetime(x, format = '%Y-%m-%d %H:%M:%S'))
        data['TenureInMonths'] = (self.reference_date - data['FirstCommStartDate']).dt.days // 30.4
        idx = data.index[data['LastContractUpgradeDate'].isnull()].tolist()
        data.loc[idx, 'LastContractUpgradeDate'] = data.loc[idx, 'FirstCommStartDate']
        data['TenureInMonthsUpgrade'] = (self.reference_date - data['LastContractUpgradeDate']).dt.days // 30.4
        int_cols = ['TenureInMonths', 'TenureInMonthsUpgrade']
        data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy())
        self.features = data
        self.get_valid_days()

    def get_active_days(self):
        print('Getting active days...')
        self.logger.info('Getting active days...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        active_days = read_sqlfile_to_df(self.filepath + 'sql_query/activeDays.sql', params)
        if len(active_days) > 0:
            data = pd.merge(data, active_days, how = 'left', on = ['AccountCode', 'Environment'])
            data.loc[data.ActiveDays.isnull(), 'ActiveDays'] = 0
            data['LoginFreq'] = data['ActiveDays'] / data['valid_activity_days']
        self.features = data

    def get_divisionstatistics_financial(self):
        print('Getting Division Statistics financial...')
        self.logger.info('Getting Division Statistics financial...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        ds_fin = read_sqlfile_to_df(self.filepath + 'sql_query/bookkeepingDivisionStatistics.sql', params)
        if len(ds_fin) > 0:
            data = pd.merge(data, ds_fin, how = 'left', on = ['AccountCode', 'Environment'])
            bin_cols = ["Big3BankAccount", "BankLink", "SalesInvoice", "CurrencyCount"]
            data[['Has' + i for i in bin_cols]] = transform.binary_columns(data[bin_cols].copy())
            
            int_cols = ["GLTransactions", "PurchaseEntryCount", "BankEntryCount", 
                        "SalesEntryCount","AccountCount", "CashEntryCount", 
                        "GeneralJournalEntryCount", "NumActivatedDivisions"]
            data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy())
        self.features = data
    
    def get_mobile(self):
        print('Getting mobile...')
        self.logger.info('Getting mobile...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        mobile = read_sqlfile_to_df(self.filepath + 'sql_query/mobile.sql', params)
        if len(mobile) > 0:
            data = pd.merge(data, mobile, how = 'left', on = ['AccountCode', 'Environment'])
            data['RecentUseMobile'] = data.UseMobile.notnull()
        self.features = data
        
    def get_holiday_season(self):
        self.features['IsHolidaySeason'] = False
        holiday_months = [1, 5, 7, 8, 9]
        m = pd.to_datetime(self.reference_date).month
        if m in holiday_months:
            self.features['IsHolidaySeason'] = True
        
    
    def get_downgrades(self):
        data = self.features.copy()
        params = [self.end_date, self.config['country'], self.config['product'],
                  self.reference_date]
        downgrades = read_sqlfile_to_df(self.filepath + 'sql_query/downgrades.sql', params)
        if len(downgrades) > 0:
            self.features = pd.merge(data, downgrades, how = 'left', on = ['AccountCode', 'Environment'])
        
    def get_label(self):
        print('Getting label...')
        self.logger.info('Getting label...')
        if self.config['label'] == 'Churned':
            self.features['Churned'] = timeframe.creat_churn_indicator(self.features, self.end_date)
        elif self.config['label'] == 'Downgraded':
            self.get_downgrades()
            self.features['Downgraded'] = timeframe.create_downgrade_indicator(self.features)
        
    def get_active_accountant(self):
        data = self.features.copy()
        print('Getting active accountants...')
        self.logger.info('Getting active accountants...')
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        active_accountants = read_sqlfile_to_df(self.filepath + 'sql_query/activeAccountants.sql', params)
        if len(active_accountants) > 0:
            data = pd.merge(data, active_accountants, how = 'left', on = ['AccountCode', 'Environment'])
            data.loc[data.AccountantDaysActive.isnull(), 'AccountantDaysActive'] = 0
            data['Accountant'] = 'NotLinked'
            data.loc[data.AccountantDaysActive > 0, 'Accountant'] = 'Active'
            data.loc[(data.AccountantOrLinked == True) & (data.AccountantDaysActive == 0), 'Accountant'] = 'Inactive'
            data['EntrepreneurActive'] = (data.EntrepreneurDaysActive > 0) & (data.EntrepreneurDaysActive.notnull())
        self.features = data
    
    def get_pageviews(self):
        print('Getting pageviews...')
        self.logger.info('Getting pageviews...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        pageviews = read_sqlfile_to_df(self.filepath + 'sql_query/pageviews.sql', params)
        if len(pageviews) > 0:
            data = pd.merge(data, pageviews, how = 'left', on = ['AccountCode', 'Environment'])
            df_p = data['Pageviews'].to_frame(name='Pageviews')     
            data['LogPageviews'] = transform.log1p_columns(df_p.copy())
        self.features = data