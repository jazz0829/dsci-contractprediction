"""
logistic regression
"""

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from common.stratified_cross_validator import StratifiedCrossValidator

class LR():
    """ logistic regression object """
    def train(self, data, params):
        """ train model """
        lrm = LogisticRegression(regParam=params, weightCol='class_weight')
        pipeline = Pipeline(stages=[lrm])
        model = pipeline.fit(data)
        return model

    def predict(self, data, model):
        """ predict based on new model """
        prediction = model.transform(data)
        return prediction

    def param_grid(self, data):
        """ cross validation to find best params """
        tuned_parameters = [0.1, 1, 10] # regParam
        model = LogisticRegression(weightCol='class_weight')
        pipeline = Pipeline(stages=[model])
        param_grid = ParamGridBuilder().addGrid(model.regParam, tuned_parameters).build()
        evaluator = BinaryClassificationEvaluator()
        cross_val = StratifiedCrossValidator(estimator=pipeline, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=5)
        cv_model = cross_val.fit(data)
        avg_metrics = cv_model.avgMetrics
        best_params = np.asscalar(np.array([value for key, value in cv_model.bestModel.stages[-1].extractParamMap().items() if key.name == 'regParam']))
        best_auc = max(avg_metrics)
        return best_params, best_auc
