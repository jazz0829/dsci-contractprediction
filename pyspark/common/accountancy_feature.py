from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, countDistinct, col, lower, upper, lit, first, when, row_number, log, datediff, date_add, greatest, coalesce
from pyspark.sql.functions import max as fmax, min as fmin, sum as fsum
from pyspark.sql.types import DateType
from dateutil.relativedelta import relativedelta

from common.common_feature import CommonFeature
import logging


class AccountancyFeature(CommonFeature):

    def get_accountancy_linked_companies(self, reference_date):
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        accounts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Accounts/')\
            .select(lower(col('accountcode')).alias('account_code'),
                    lower(col('accountid')).alias('account_id'),
                    lower(col('environment')).alias('environment'))

        users = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Users/')\
                    .alias('u')\
                    .select(trim((col('u.accountid'))).alias('account_id'),
                            trim(lower(col('u.userid'))).alias('user_id'))\
                    .join(accounts.alias('a'), ['account_id'])

        activitydaily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                        .select('date',col('divisioncode').alias('division_code'),
                                trim(lower(col('activityid'))).alias('activity_id'),
                                trim(lower(col('userid'))).alias('user_id'),
                                col('quantity').alias('quantity'),
                                trim(lower(col('accountid'))).alias('account_id'))\
                        .filter(col('date')>=start_date)\
                        .filter(col('date') <= end_date)\
                        .filter(col('activity_id') == 1)

        accountancy_linked_companies = activitydaily.alias('ad')\
                        .join(users.alias('u'), ['user_id'])\
                        .filter(lower(col('ad.account_id')) != lower(col('u.account_id')))\
                        .filter(lower(col('environment')).isin(self.environment))\
                        .groupBy('account_code', 'environment')\
                        .agg(fsum('quantity').alias('page_views_on_linked_companies'),
                              countDistinct('date').alias('active_days_on_linked_companies'),
                               countDistinct('division_code').alias('num_active_linked_companies'))
        return accountancy_linked_companies

    def get_accountancy(self, reference_date):
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        activitydaily =  self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                          .select('date',
                                trim(lower(col('activityid'))).alias('activity_id'),
                                col('quantity').alias('quantity'),
                                trim(lower(col('accountcode'))).alias('account_code'),
                                trim(lower(col('environment'))).alias('environment'))
        
        w_env_div = Window.partitionBy(col('environment'), col('account_code')).orderBy(col('date').desc())
        contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('packagecode'))).alias('package_code'), 'date')\
            .filter((col('date') <= end_date))\
            .withColumn('date_number', row_number().over(w_env_div))

        continuousmonitoring = activitydaily.alias('activity_daily')\
                       .filter((col('activity_id') == 5017) | (col('activity_id') == 5018) | (col('activity_id') ==5019))\
                       .filter((col('date') >= start_date) & (col('date') <= end_date))\
                       .groupby('activity_daily.account_code', 'activity_daily.environment')\
                       .agg(fsum('quantity').alias('continuous_monitoring'))

        scanning = activitydaily\
                       .filter((col('activity_id') == 5003) | (col('activity_id') == 5005))\
                       .filter((col('date') >= start_date) & (col('date') <= end_date))\
                       .groupby('account_code', 'environment')\
                       .agg(fsum('quantity').alias('scanning'))

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(lower(col('product')).alias('product'), trim(lower(col('environment'))).alias('environment'),
                                 trim(lower(col('packagecode'))).alias('package_code'))

        processmanagement = activitydaily\
                       .filter((col('activity_id') == 5004))\
                       .filter((col('date') >= start_date) & (col('date') <= end_date))\
                       .groupby('account_code', 'environment')\
                       .agg(fsum('quantity').alias('process_management'))

        acc_activity_joined = activitydaily.alias('activity_daily')\
                             .join(contracts.alias('contracts'), ['environment', 'account_code'], 'left')\
                             .join(package_classification, ['environment','package_code'])\
                             .join(continuousmonitoring.alias('CM'), ['account_code','environment'], 'left')\
                             .join(scanning.alias('SR'), ['account_code','environment'], 'left' )\
                             .join(processmanagement.alias('PM'), ['account_code','environment'], 'left')

        grouped_results = acc_activity_joined\
                           .filter(col('environment').isin(self.environment))\
                           .filter((col('activity_daily.date') >= start_date) & (col('activity_daily.date') <= end_date))\
                           .filter(col('contracts.date_number') == 1)\
                           .filter(col('product') == self.product)\
                           .groupBy(['account_code', 'environment', 'continuous_monitoring', 'scanning', 'process_management'])\
                           .count().orderBy(col('account_code')).drop('count')  #Without this, the method will return a groupby_object :(
        return grouped_results
