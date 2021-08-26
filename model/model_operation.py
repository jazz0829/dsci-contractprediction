import pandas as pd
import numpy as np
from sklearn.externals import joblib
from datetime import datetime
from os import path
from model.logistic_regression import LR
from model.xgboost import XGBoost
from function import read_utils
from function import write_utils
from sklearn.metrics import roc_auc_score


class ModelOperation():
    def __init__(self, config):
        self.config = config
        if config['algorithm'] == 'LogisticRegression':
            self.alg_obj = LR()
        elif config['algorithm'] == 'XGBoost':
            self.alg_obj = XGBoost()
        self.model_file = read_utils.get_model_files(self.config['model_folder'], 
                                            self.config['model_file_prefix'])   
        self.model = None
       

    def select_and_train(self, training_data):
        X = training_data[self.config['predictors']].copy()
        categorical_cols = list(X.select_dtypes(exclude=[np.number, bool]))
        X = pd.get_dummies(X, categorical_cols)
        Y = training_data[self.config['label']]
        best_params, best_auc = self.alg_obj.grid_search(X, Y)
        if best_auc > self.config['accepted_auc'] or len(self.model_file) == 0:        
            self.model = self.alg_obj.train(X, Y, best_params)     
            self.save_to_file()    
        return best_auc
    
    def predict(self, new_data):
        X_new = new_data[self.config['predictors']].copy()
        categorical_cols = list(X_new.select_dtypes(exclude=[np.number, bool]))
        X_new = pd.get_dummies(X_new, categorical_cols)        
        probs = self.alg_obj.predict(X_new, self.model)
        scores = self.alg_obj.probability_to_score(probs)
        return scores
    
    def evaluate(self, eval_data):
        X_eval = eval_data[self.config['predictors']].copy()
        categorical_cols = list(X_eval.select_dtypes(exclude=[np.number, bool]))
        X_eval = pd.get_dummies(X_eval, categorical_cols) 
        Y_eval= eval_data[self.config['label']]
        probs = self.alg_obj.predict(X_eval, self.model)
        roc_auc = roc_auc_score(Y_eval, probs)
        return roc_auc
    
    def validate(self, labels, probs):
        return roc_auc_score(labels, probs)
    
    def save_to_file(self):        
        old_models = read_utils.get_model_files(self.config['model_folder'],
                                            self.config['model_file_prefix'])
        write_utils.move_old_models(self.config['model_folder'], old_models)
        self.model_file = [self.config['model_file_prefix'] + datetime.today().strftime('%Y%m%d') + '.pkl']
        joblib.dump(self.model, path.join(self.config['model_folder'], self.model_file[0]))
    
    def load_model_file(self): 
        if len(self.model_file) == 0:
            raise ValueError('Trained model not found')
        elif len(self.model_file) > 1:
            raise ValueError('Trained model not unique')
        self.model = joblib.load(path.join(self.config['model_folder'], self.model_file[0]))
        
