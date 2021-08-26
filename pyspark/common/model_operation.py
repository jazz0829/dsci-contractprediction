"""
model operations
"""
from functools import reduce
import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from common.logistic_regression import LR

class ModelOperation():
    """ model operations object"""
    def __init__(self, spark, config):
        """ initialization """
        self.unique_id = config.get('unique_id')
        self.config = config
        self.spark = spark
        if config['algorithm'] == 'LogisticRegression':
            self.alg_obj = LR()
        self.model = None

    def get_pk(self, df_in):
        """ create primary key based on uniqueid columns """
        df_in = df_in.withColumn('pk', F.concat_ws('^', *self.unique_id))
        return df_in

    def preprocessing(self, df_in, features, label, fillna=True):
        """ preprocessing dataframe """
        df_in = df_in.withColumn(label, col(label).cast(T.IntegerType()))
        balancing_ratio = (df_in.count() - df_in.filter(df_in[label] == 1).count())/df_in.count()
        balancing_ratio_udf = F.udf(lambda label: 1*balancing_ratio if label == 1 else 1 * (1-balancing_ratio))
        df_in = df_in.withColumn('class_weight', balancing_ratio_udf(df_in[label]).cast(T.DoubleType()))
        if fillna:
            df_in = reduce(lambda y, x: y.withColumn(x, F.when((col(x).isNull() | F.isnan(col(x))), lit(0)).otherwise(col(x))), features, df_in)
        return df_in

    def get_dummy(self, df_in, categorical_cols):
        """ create dummies for categorical columns """
        indexer = [StringIndexer(inputCol=column, outputCol=column+'_index').fit(df_in) for column in categorical_cols]
        pipeline = Pipeline(stages=indexer)
        data = pipeline.fit(df_in).transform(df_in)
        data = reduce(lambda y, x: y.withColumn(x, col(x+'_index')), categorical_cols, data)
        return data

    def transform(self, df_in, features, label):
        """ create dense vector """
        return df_in.rdd.map(lambda r: [Vectors.dense([r[column] for column in features]), r[label], r['class_weight'], r['pk']])\
            .toDF(['features', 'label', 'class_weight', 'pk'])

    def select_and_train(self):
        """ select best params with cross validation and train a model with best params """
        features = self.config['predictors']
        label = self.config['label']
        training_data = self.spark.read.parquet(*[self.config['training_data_path']])
        categorical_cols = [col[0] for col in training_data.dtypes if col[1].startswith('string') and col[0] in features]
        numerical_cols = [col[0] for col in training_data.dtypes if not col[1].startswith('string') and not col[1].startswith('boolean') and col[0] in features]
        data = self.get_pk(training_data)
        data = self.preprocessing(data, numerical_cols, label)
        data = self.get_dummy(data, categorical_cols)
        data = self.transform(data, features, label)
        best_params, best_auc = self.alg_obj.param_grid(data)
        if best_auc > self.config['accepted_auc']: #TODO check for model existance and if no model then always train
            self.model = self.alg_obj.train(data, best_params)
            self.model.write().overwrite().save(self.config['model_path'])
        return best_auc, best_params

    def predict(self, new_data):
        """ prepare dataset, load model and does predictions """
        features = self.config['predictors']
        label = self.config['label']
        test_data = new_data
        categorical_cols = [col[0] for col in test_data.dtypes if col[1].startswith('string') and col[0] in features]
        numerical_cols = [col[0] for col in test_data.dtypes if not col[1].startswith('string') and not col[1].startswith('boolean') and col[0] in features]
        data = self.get_pk(test_data)
        data = self.preprocessing(data, numerical_cols, label)
        data = self.get_dummy(data, categorical_cols)
        data = self.transform(data, features, label)
        self.load_model_file() # loads the model
        scores = self.alg_obj.predict(data, self.model)
        return scores

    def load_model_file(self):
        """
        loads model
        TODO: check for existance of model
        """
        self.model = PipelineModel.load(self.config['model_path'])
