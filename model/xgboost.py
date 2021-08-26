import xgboost as xgb
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import GridSearchCV
import numpy as np

class XGBoost():
    def train(self, X, Y, params):
        weight = len(Y[Y] == 1)/ len(Y[Y] == 0)
        model = xgb.XGBClassifier(objective='binary:logistic', 
                                  scale_pos_weight = weight, learning_rate = 0.05,
                                  max_delta_step = 4, subsample = 0.6,
                                  gamma = 0.1, colsample_bytree = 0.6,
                                  min_child_weight= params['min_child_weight'],
                                  max_depth = params['max_depth'],
                                  reg_lambda = params['reg_lambda'],
                                  n_estimators = 50, early_stopping_rounds = 5)
        model.fit(X, Y, eval_metric="auc", verbose=True)
        return model
    
    def predict(self, X_new, model):
        return model.predict_proba(X_new)[:,1] 
    
    def grid_search(self, X, Y):
        weight = len(Y[Y] == 1)/ len(Y[Y] == 0)
        tuned_parameters = {'max_depth':[3, 4, 5], 'reg_lambda':[1, 2], 
                            'min_child_weight': [5, 10, 20]}
        estimator = xgb.XGBClassifier(objective='binary:logistic', 
                                      scale_pos_weight = weight, learning_rate = 0.05,
                                      max_delta_step = 4, subsample = 0.6,
                                      gamma = 0.1, colsample_bytree = 0.6,
                                      n_estimators = 50, early_stopping_rounds = 5)
        clf = GridSearchCV(estimator, tuned_parameters, cv=StratifiedKFold(
                n_splits=5, random_state=0, shuffle=True), scoring='roc_auc')
                        
        clf.fit(X, Y)
        best_params = clf.best_params_
        best_auc = max(clf.cv_results_['mean_test_score'])
        return best_params, best_auc

    def probability_to_score(self, probs):
        scores = np.round((1 - probs) * 100)
        return scores


