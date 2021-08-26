from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, countDistinct, col, lower, upper, lit, first, when, row_number, log, datediff, date_add, greatest, coalesce
from pyspark.sql.functions import max as fmax, min as fmin, sum as fsum
from pyspark.sql.types import DateType
from dateutil.relativedelta import relativedelta

from common.common_feature import CommonFeature
import logging


class ManufacturingFeature(CommonFeature):

    def _get_contracts(self):
        self.contracts = spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('packagecode'))).alias('package_code'), 'date')

    def _get_activity_daily(self):
        self.activity_daily =  spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                        .select('date',col('divisioncode').alias('division_code'),
                                trim(lower(col('activityid'))).alias('activity_id'),
                                trim(lower(col('userid'))).alias('user_id'),
                                col('quantity').alias('quantity'),
                                trim(lower(col('accountcode'))).alias('account_code'),
                                trim(lower(col('environment'))).alias('environment'),
                                trim(lower(col('accountid'))).alias('account_id'))\

    def get_active_user_divisions(self, reference_date):
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        activitydaily =  self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                         .select('date', col('divisioncode').alias('division_code'),
                                 trim(lower(col('activityid'))).alias('activity_id'),
                                 trim(lower(col('userid'))).alias('user_id'),
                                 trim(lower(col('accountcode'))).alias('account_code'),
                                 trim(lower(col('environment'))).alias('environment'))\
                         .select('environment', 'account_code', 'user_id', 'division_code', 'date', 'activity_id')\
                         .filter((col('date') >= start_date) & (col('date') <= end_date))\
                         .filter(col('activity_id') == 1)

        w_env_div = Window.partitionBy(col('environment'), col('account_code')).orderBy(col('date').desc())
        contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
                    .select(trim(lower(col('environment'))).alias('environment'),
                            trim(lower(col('accountcode'))).alias('account_code'),
                            trim(lower(col('packagecode'))).alias('package_code'), 'date')\
                    .filter(col('date') <= end_date)\
                    .withColumn('date_number', row_number().over(w_env_div))

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(trim(lower(col('product'))).alias('product'),
                                 trim(lower(col('environment'))).alias('environment'),
                                 trim(lower(col('packagecode'))).alias('package_code'))\
                         .filter(col('product') == self.product)


        active_users_division = activitydaily\
         .join(contracts, ['environment', 'account_code'], how='left')\
         .join(package_classification, ['environment', 'package_code'], how='left')\
         .filter(col('environment').isin(self.environment))\
         .filter(col('date_number') == 1)\
         .groupby(['environment', 'account_code'])\
         .agg(countDistinct('user_id').alias('active_users'),
              countDistinct('division_code').alias('active_divisions'))

        return active_users_division

    def get_manuacturing_activity(self, reference_date):
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        activitydaily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                .select(trim(lower(col('environment'))).alias('environment'),
                        trim(lower(col('accountcode'))).alias('account_code'),
                        trim(lower(col('userid'))).alias('user_id'),
                        trim(lower(col('divisioncode'))).alias('division_code'),
                        trim(lower(col('quantity'))).alias('quantity'),
                        'date',
                        trim(lower(col('activityid'))).alias('activity_id'))\
                .filter((col('date') >= start_date) & (col('date') <= end_date))

        w_env_div = Window.partitionBy(col('environment'), col('account_code')).orderBy(col('date').desc())
        contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'),
                    trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('packagecode'))).alias('package_code'),
                    'date')\
            .filter(col('date') <= end_date)\
            .withColumn('date_number', row_number().over(w_env_div))

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(trim(lower(col('product'))).alias('product'),
                                 trim(lower(col('environment'))).alias('environment'),
                                 trim(lower(col('packagecode'))).alias('package_code'))

        basic_quantity = activitydaily\
                 .select('environment', 'account_code', 'quantity', 'activity_id', 'date')\
                 .filter((col('activity_id') == 5175) | \
                         (col('activity_id') == 5060) | \
                         (col('activity_id') == 5061) | \
                         (col('activity_id') == 5062) | \
                         (col('activity_id') == 5058) | \
                         (col('activity_id') == 5173) | \
                         (col('activity_id') == 5261) | \
                         (col('activity_id') == 5259)) \
                  .groupBy('account_code', 'environment')\
                  .agg(fsum(col('quantity')).alias('manufacturing_basic'))

        advanced_quantity = activitydaily\
                 .select('environment', 'account_code', 'quantity', 'activity_id', 'date')\
                 .filter((col('activity_id') == 5044) | \
                         (col('activity_id') == 5045) | \
                         (col('activity_id') == 5046) | \
                         (col('activity_id') == 5056) | \
                         (col('activity_id') == 5168))\
                  .groupBy('account_code', 'environment')\
                  .agg(fsum(col('quantity')).alias('manufacturing_advanced'))

        manufacturing_activity = activitydaily\
                         .join(contracts.alias('contracts'), ['environment', 'account_code'], how='left')\
                         .join(package_classification.alias('package_classification'), ['environment', 'package_code'])\
                         .join(basic_quantity, ['account_code', 'environment'], how='left')\
                         .join(advanced_quantity, ['account_code', 'environment'], how='left')\
                         .filter(col('environment').isin(self.environment))\
                         .filter(col('contracts.date_number') == 1)\
                         .filter(col('package_classification.product') == self.product)\
                         .groupBy(['account_code', 'environment', 'manufacturing_basic', 'manufacturing_advanced'])\
                         .count().orderBy(col('account_code')).drop('count')

        return manufacturing_activity
