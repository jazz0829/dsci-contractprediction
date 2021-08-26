"""
Spark by default does not have Stratified folding so extending cross validator
source: https://github.com/interviewstreet/spark-stratifier
"""
from functools import reduce
import numpy as np
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel

class StratifiedCrossValidator(CrossValidator):
    """
    Spark by default does not have Stratified folding so extending cross validator
    """
    def stratify_data(self, dataset):
        """
        Returns an array of dataframes with the same ratio of passes and failures.
        Currently only supports binary classification problems.
        """

        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        nFolds = self.getOrDefault(self.numFolds)
        split_ratio = 1.0 / nFolds

        passes = dataset[dataset['label'] == 1]
        fails = dataset[dataset['label'] == 0]

        pass_splits = passes.randomSplit([split_ratio for i in range(nFolds)])
        fail_splits = fails.randomSplit([split_ratio for i in range(nFolds)])

        stratified_data = [pass_splits[i].unionAll(fail_splits[i]) for i in range(nFolds)]

        return stratified_data

    def _fit(self, dataset):
        """
        Fits dataset
        """
        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        eva = self.getOrDefault(self.evaluator)
        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        metrics = [0.0] * numModels

        stratified_data = self.stratify_data(dataset)

        for i in range(nFolds):
            train_arr = [x for j, x in enumerate(stratified_data) if j != i]
            train = reduce((lambda x, y: x.unionAll(y)), train_arr)
            validation = stratified_data[i]

            models = est.fit(train, epm)

            for j in range(numModels):
                model = models[j]
                metric = eva.evaluate(model.transform(validation, epm[j]))
                metrics[j] += metric/nFolds

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)

        bestModel = est.fit(dataset, epm[bestIndex])
        return self._copyValues(CrossValidatorModel(bestModel, metrics))
