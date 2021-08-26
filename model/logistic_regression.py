from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedKFold
import numpy as np

class LR():

    def train(self, X, Y, params):
        model = LogisticRegression(C = params['C'], class_weight = 'balanced')
        model.fit(X, Y)
        return model
    
    def predict(self, X_new, model):
        return model.predict_proba(X_new)[:,1] 
    
        
    def grid_search(self, X, Y):
        tuned_parameters = {'C':[0.1, 1, 10]}
        clf = GridSearchCV(LogisticRegression(class_weight = 'balanced'), 
                           tuned_parameters, cv=StratifiedKFold(n_splits=5,
                           random_state=0, shuffle=True), scoring='roc_auc')
                        
        clf.fit(X, Y)
        best_params = clf.best_params_
        best_auc = max(clf.cv_results_['mean_test_score'])
        return best_params, best_auc

    
    def probability_to_score(self, probs):
        scores = np.round((1 - probs) * 100)
        return scores
