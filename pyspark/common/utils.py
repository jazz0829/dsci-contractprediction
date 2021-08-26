""" utility functions """

import datetime
from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T

def write_train_statistics(spark, config, auc, reg_param):
    """
    write train statistics to a file.
    """
    base_path = config['statistics_path']
    df_out = spark.createDataFrame(Row(**row) for row in [config])\
        .select(F.col('country').alias('environment'),
                F.col('product').alias('product'),
                F.col('phase').alias('phase'),
                F.col('algorithm').alias('algorithm'),
                F.col('past_date').alias('past_date'),
                F.col('category').alias('category'))\
        .withColumn('auc', F.lit(auc))\
        .withColumn('reg_param', F.lit(reg_param))\
        .withColumn('train_date', F.lit(datetime.datetime.now()))\
        .withColumn('reference_date', F.col('past_date'))\
        .withColumn('category', F.col('category'))
    df_out.repartition(1).write.mode('append').parquet(base_path)

def write_churn_output(config, scores):
    """
    write prediction scores into S3 file.
    """
    base_path = config['predictions_path']
    split0_udf = F.udf(lambda value: round(value[0].item()*100), T.IntegerType())
    df_out = scores.select(['pk', 'probability'])\
        .withColumn('pk', F.split(scores['pk'], r'\^'))\
        .withColumn(config['unique_id'][0], F.col('pk')[0])\
        .withColumn(config['unique_id'][1], F.col('pk')[1])\
        .withColumn(config['unique_id'][2], F.col('pk')[2])\
        .withColumn('health_score', split0_udf('probability'))\
        .withColumn('reference_date', F.lit(config['current_date']))\
        .withColumn('predict_date', F.lit(datetime.datetime.now()))\
        .withColumn('phase', F.lit(config['phase']))\
        .drop('pk', 'probability')
    df_out.repartition(1).write.mode('append').parquet(base_path)

def get_feature_object():
    """ placeholder """
    print('dummy')
