from datetime import timedelta, datetime
import pandas as pd
from function.product_feature_factory import get_product_data
from function.read_utils import read_sqlfile_to_df
from function import write_utils, timeframe
import os, logging

class Dataset():
    def __init__(self, arg_dict, config):
        self.filepath = '' if 'cig-ds-contractprediction\\contractprediction' in os.getcwd() else 'contractprediction/'
        self.arg_dict = self.refine_reference_dates(arg_dict, config)
        self.config = config
        self.logger = logging.getLogger(__name__)

    def refine_reference_dates(self, arg_dict, config):
        if 'current_date' not in arg_dict.keys() and 'past_date' not in arg_dict.keys():
            res = read_sqlfile_to_df(self.filepath + 'sql_query/lastUpdatedDate.sql')
            arg_dict['current_date']  = pd.to_datetime(res.iloc[0].values[0])
            arg_dict['past_date'] = arg_dict['current_date'] - timedelta(days=config['window_contract'])
        elif 'current_date' in arg_dict.keys() and 'past_date' not in arg_dict.keys():
            arg_dict['current_date'] = pd.to_datetime(arg_dict['current_date'] )
            arg_dict['past_date'] = arg_dict['current_date'] - timedelta(days=config['window_contract'])
        elif 'current_date' not in arg_dict.keys() and 'past_date' in arg_dict.keys():
            arg_dict['past_date'] = pd.to_datetime(arg_dict['past_date'] )
            res = read_sqlfile_to_df(self.filepath + 'sql_query/lastUpdatedDate.sql')
            latest_date = pd.to_datetime(res.iloc[0].values[0])
            future_date = arg_dict['past_date'] + timedelta(days=config['window_contract'])
            arg_dict['current_date'] = min([latest_date, future_date])
        else:
            arg_dict['current_date'] = pd.to_datetime(arg_dict['current_date'] )
            arg_dict['past_date'] = pd.to_datetime(arg_dict['past_date'] )
            
        return arg_dict

    def get_training_data(self, num_windows):
        training_data = pd.DataFrame()
        shifted_window_date = self.arg_dict['past_date']
        for i in range(num_windows):
            print("Shifted window date: {}".format(shifted_window_date))
            self.logger.info("Shifted window date: {}".format(shifted_window_date))
            window_data = get_product_data(shifted_window_date, self.config)
            training_data = training_data.append(window_data, ignore_index=True)
            shifted_window_date = shifted_window_date - timedelta(days=self.config['window_contract'])
        return training_data 

    def get_new_data(self):
        new_data = get_product_data(self.arg_dict['current_date'], self.config)
        return new_data  

    def get_evaluation_data(self):
        eval_date = self.arg_dict['past_date'] - pd.DateOffset(days=round(1.5 *
                                 self.config['window_contract']))
        eval_data = get_product_data(eval_date, self.config)
        return eval_data 

    def output_exists(self):
        params = [self.arg_dict['current_date'], self.config['country'], 
                  self.config['product'], self.config['phase']]
        if self.arg_dict['category'] == 'churn':
            output = read_sqlfile_to_df(self.filepath + 'sql_query/healthScoreDateExists.sql', params)
        elif self.arg_dict['category'] == 'downgrade':
            output = read_sqlfile_to_df(self.filepath + 'sql_query/downgradeDateExists.sql', params)
        if len(output) > 0:
            return True
        else:
            return False

    def get_old_scores_labels(self):
        old_date = self.arg_dict['current_date'] - pd.DateOffset(months=self.config['period'])
        if self.config['label'] == 'Churned':
            params = [old_date, self.config['country'], self.config['product'], self.config['phase']]  
            old_scores_labels = read_sqlfile_to_df(self.filepath + 'sql_query/oldScoresChurns.sql', params)
            predict_date = pd.to_datetime(old_scores_labels.loc[0, 'PredictDate'])
            end_date = predict_date + pd.DateOffset(months=self.config['period'])
            date_cols = ["LatestCommFinalDate","FullCancellationRequestDate"]
            old_scores_labels[date_cols] = old_scores_labels[date_cols].applymap(lambda x : pd.to_datetime(x, format = '%Y-%m-%d %H:%M:%S'))
            old_scores_labels['Label'] = timeframe.creat_churn_indicator(old_scores_labels, end_date)     
            old_scores_labels['Prob'] = (100 - old_scores_labels['HealthScore']) / 100        
        elif self.config['label'] == 'Downgraded':
            params = [old_date, self.config['country'], self.config['product']]  
            old_scores_labels = read_sqlfile_to_df(self.filepath + 'sql_query/oldScoresDowngrades.sql', params)
            predict_date = pd.to_datetime(old_scores_labels.loc[0, 'PredictDate'])
            end_date = predict_date + pd.DateOffset(months=self.config['period'])
            date_cols = ["DowngradeDate"]
            old_scores_labels[date_cols] = old_scores_labels[date_cols].applymap(lambda x : pd.to_datetime(x, format = '%Y-%m-%d %H:%M:%S'))
            old_scores_labels['Label'] = timeframe.create_downgrade_indicator(old_scores_labels)     
            old_scores_labels['Prob'] = (100 - old_scores_labels['Score']) / 100
        return old_scores_labels

    def write_output_to_db(self, reference_date, new_data, scores):
        filepath = None if self.filepath else 'output_prediction.csv'
        if self.arg_dict['category'] == 'churn':
            self.write_churn_output(new_data, scores, filepath)
        elif self.arg_dict['category'] == 'downgrade':
            self.write_downgrade_output(new_data, scores, filepath)

    def write_churn_output(self, new_data, scores, filepath = None):
        output_df = new_data[['Environment', 'AccountCode', 'PackageCode']].copy()
        output_df['HealthScore'] = scores
        output_df['ReferenceDate'] = self.arg_dict['current_date']
        output_df['PredictDate'] = datetime.now()
        output_df['Phase'] = self.config['phase']
        if filepath == None:
            write_utils.write_to_sql(output_df, 'HealthScore', 'publish') 
        else:
            if os.path.isfile(filepath) :
                with open(filepath, 'a') as f:
                    output_df.to_csv(f, header=False)
            else:
                output_df.to_csv(filepath)

    def write_downgrade_output(self, new_data, scores, filepath = None):
        output_df = new_data[['Environment', 'AccountCode', 'PackageCode']].copy()
        output_df['Score'] = scores
        output_df['ReferenceDate'] = self.arg_dict['current_date']
        output_df['PredictDate'] = datetime.now()
        output_df['Phase'] = self.config['phase']
        if filepath == None:
            write_utils.write_to_sql(output_df, 'ISUsageScore', 'publish') 
        else:
            if os.path.isfile(filepath) :
                with open(filepath, 'a') as f:
                    output_df.to_csv(f, header=False)
            else:
                output_df.to_csv(filepath)

    def write_train_statistics(self, training_data, auc):
        filepath = None if self.filepath else 'train_statistics.csv'
        stat_df = training_data[['Environment', 'Product']].head(1).copy() 
        stat_df['Environment'] = self.config['country']
        stat_df['Phase'] = self.config['phase']
        stat_df['Category'] = self.arg_dict['category']
        stat_df['Algorithm'] = self.config['algorithm']
        stat_df['ReferenceDate'] = self.arg_dict['past_date']
        stat_df['TrainDate'] = datetime.now()
        stat_df['AUC'] = auc
        if filepath == None:
            write_utils.write_to_sql(stat_df, 'TrainModelStatistics', 'config') 
        else:
            if os.path.isfile(filepath) :
                with open(filepath, 'a') as f:
                    stat_df.to_csv(f, header=False)
            else:
                stat_df.to_csv(filepath)

    def write_evaluate_statistics(self, eval_data, modelname, auc):
        filepath = None if self.filepath else 'evaluate_statistics.csv'
        stat_df = eval_data[['Environment', 'Product']].head(1).copy()
        stat_df['Environment'] = self.config['country']
        stat_df['Phase'] = self.config['phase']
        stat_df['Category'] = self.arg_dict['category']
        train_date = pd.to_datetime(modelname.split('_')[1].split('.')[0])        
        stat_df['TrainDate'] = train_date
        stat_df['ReferenceDate'] = train_date - pd.DateOffset(days=round(1.5 * 
               self.config['window_contract']))
        stat_df['EvaluateDate'] = datetime.now()
        stat_df['AUC'] = auc
        if filepath == None:
            write_utils.write_to_sql(stat_df, 'EvaluateModelStatistics', 'config') 
        else:
            if os.path.isfile(filepath) :
                with open(filepath, 'a') as f:
                    stat_df.to_csv(f, header=False)
            else:
                stat_df.to_csv(filepath)

    def write_validate_statistics(self, valid_data, auc):
        filepath = None if self.filepath else 'validate_statistics.csv'
        stat_df = valid_data[['Environment', 'Product']].head(1).copy()
        stat_df['Environment'] = self.config['country']
        stat_df['Phase'] = self.config['phase']
        stat_df['Category'] = self.arg_dict['category']       
        stat_df['PredictDate'] = valid_data.loc[0, 'PredictDate']
        stat_df['ValidateDate'] = datetime.now()
        stat_df['AUC'] = auc
        if filepath == None:
            write_utils.write_to_sql(stat_df, 'ValidateModelStatistics', 'config') 
        else:
            if os.path.isfile(filepath) :
                with open(filepath, 'a') as f:
                    stat_df.to_csv(f, header=False)
            else:
                stat_df.to_csv(filepath)
