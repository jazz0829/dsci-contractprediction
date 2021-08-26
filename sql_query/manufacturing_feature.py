import pandas as pd
from function.read_utils import read_sqlfile_to_df
from function import transform
import os, logging

class ManufacturingFeature():
    def __init__(self, reference_date, config):
        self.reference_date = reference_date
        self.config = config
        self.features = None
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.logger = logging.getLogger(__name__)

    def get_manufacturing_activities(self):
        print('Getting Division Statistics wholesale...')
        self.logger.info('Getting Division Statistics wholesale...')
        data = self.features.copy()
        params = [self.reference_date, self.config['window_activities'], 
                  self.config['country'], self.config['product']]
        man_act = read_sqlfile_to_df(self.filepath + 'sql_query/manufacturingActivities.sql', params)
        if len(man_act) > 0:
            data = pd.merge(data, man_act, how = 'left', on = ['AccountCode', 'Environment'])        
            data['ManufacturingBasic'] = data['ManufacturingBasic'].fillna(0)
            data['ManufacturingAdvanced'] = data['ManufacturingAdvanced'].fillna(0)
            sum_df = (data.ManufacturingBasic  + data.ManufacturingAdvanced).to_frame('ManufacturingActities')
            data['LogManufacturingActivities'] = transform.log1p_columns(sum_df.copy())
        self.features = data

    
    def get_manufacturing_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()
        # active days
        cf_obj.get_active_days()       
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()    
        cf_obj.get_mobile()
        cf_obj.get_label()
        self.features = cf_obj.features  
        df_stock = self.features['StockCountEntry'].to_frame('StockCountEntryCount')
        self.features['LogStockCountEntryCount'] = transform.log1p_columns(df_stock)        
        # division manufacturing activities
        self.get_manufacturing_activities()
                
        return self.features
        
    