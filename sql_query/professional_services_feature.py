import pandas as pd
from function.read_utils import read_sqlfile_to_df
from function import transform
import os, logging

class ProfessionalServicesFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.logger = logging.getLogger(__name__)

    def get_ds_psa(self):
        print('Getting Division Statistics Professional services...')
        self.logger.info('Getting Division Statistics Professional services...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        ds_psa = read_sqlfile_to_df(self.filepath + 'sql_query/professionalServicesDivisionStatistics.sql', params)
        if len(ds_psa) > 0:
            data = pd.merge(data, ds_psa, how = 'left', on = ['AccountCode', 'Environment'])        
            nan_neg_cols = ["ProjectTotalTimeEntries","ProjectTotalCostEntriesCount", 
                            "ProjectTypeFixedPrice", "ProjectTypeTimeAndMaterial",
                            "ProjectTypeNonBillable", "ProjectTypePrepaidRetainer", 
                            "ProjectTypePrepaidHTB"]
            pro_cols = ["ProjectTypeFixedPrice", "ProjectTypeTimeAndMaterial",
                        "ProjectTypeNonBillable", "ProjectTypePrepaidRetainer", 
                        "ProjectTypePrepaidHTB"]
            data.loc[: ,nan_neg_cols] = transform.to_zero(data[nan_neg_cols].copy())
            data['ProjectTimeCostEntriesCount'] = data['ProjectTotalCostEntriesCount'] + data['ProjectTotalTimeEntries']
            data['ProjectsCount'] = data[pro_cols].sum(axis=1)   
            int_cols = ['ProjectTimeCostEntriesCount', 'ProjectsCount']
            data[['Log' + i for i in int_cols]] = transform.log1p_columns(data[int_cols].copy())
        self.features = data

    def get_professional_services_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()
        # active days
        cf_obj.get_active_days()       
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()    
        cf_obj.get_mobile()
        cf_obj.get_label()
        
        data = cf_obj.features.copy()
        data.SectorCode = data.SectorCode.fillna('Unknown')
        data.UseMobile = data.UseMobile.fillna(0)
        data.NumActivatedDivisions = data.NumActivatedDivisions.fillna(0)
        data.Big3BankAccount = data.Big3BankAccount.fillna(0)
        data.BankLink = data.BankLink.fillna(0)
        data.EverSentInvoice = data.EverSentInvoice.fillna(0)
        data.CurrencyCount = data.CurrencyCount.fillna(0)
        data.GLTransactions = data.GLTransactions.fillna(0)
        data.SalesInvoice = data.SalesInvoice.fillna(0)
        data.PurchaseEntryCount = data.PurchaseEntryCount.fillna(0)
        data.StockCountEntry = data.StockCountEntry.fillna(0)
        data.BankEntryCount = data.BankEntryCount.fillna(0)
        data.CashEntryCount = data.CashEntryCount.fillna(0)
        data.SalesEntryCount = data.SalesEntryCount.fillna(0)
        data.GeneralJournalEntryCount = data.GeneralJournalEntryCount.fillna(0)
        data.AccountCount = data.AccountCount.fillna(0)
        self.features = data
        
        # division statistics professional services
        self.get_ds_psa()

        return self.features
        
    