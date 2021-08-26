from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, countDistinct, col, lower, upper, lit, first, when, row_number, log, datediff, date_add, greatest, coalesce
from pyspark.sql.functions import max as fmax, min as fmin, sum as fsum
from pyspark.sql.types import DateType, IntegerType
from dateutil.relativedelta import relativedelta

from common.common_feature import CommonFeature


class OtherFeature(CommonFeature):

    def __init__(self, *args, **kwargs):
        super(OtherFeature, self).__init__(*args, **kwargs)
        self.phase = conf.get('phase').lower()

    def healthscore_date_exists(self, reference_date):

        datetransform = udf(lambda x: datetime.strptime(x, '%m-%d-%y %I:%M %p'), DateType())

        health_score = self.spark.read.csv('s3://cig-prod-domain-bucket/Data/Publish.HealthScore/', header=True)\
               .select(trim(lower(col('environment'))).alias('environment'),
                       trim(lower(col('packagecode'))).alias('package_code'),
                       col('predictdate').alias('predict_date'),
                       col('referencedate').alias('reference_date'),
                       col('healthscore').cast(IntegerType()).alias('health_score'),
                       trim(lower(col('phase'))).alias('phase'))\
                .withColumn("reference_date", datetransform(col('reference_date')))\
                .withColumn("predict_date", datetransform(col('predict_date')))

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(trim(lower(col('product'))).alias('product'), trim(lower(col('environment'))).alias('environment'),
                                 trim(lower(col('packagecode'))).alias('package_code'))

        health_score_date_exists = health_score\
                           .join(package_classification, ['environment', 'package_code'], how='left')\
                           .filter(col('product') == self.product)\
                           .filter(col('environment').isin(self.environment))\
                           .filter(col('phase') == self.phase)\
                           .filter(col('reference_date') == reference_date)\
                           .orderBy(col('health_score').desc())\
                           .first()

        return health_score_date_exists

    def downgrade_date_exists(self, reference_date):
        isusage_score = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ISUsageScore/')\
                .select(trim(lower(col('environment'))).alias('environment'),
                        trim(lower(col('packagecode'))).alias('package_code'),
                        trim(lower(col('phase'))).alias('phase'),
                        col('score').cast(IntegerType()).alias('score'),
                        col('referencedate').cast(DateType()).alias('reference_date'))
        package_classification = spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(lower(col('product')).alias('product'), trim(lower(col('environment'))).alias('environment'),
                                 trim(lower(col('packagecode'))).alias('package_code'))

        downgrade_date_exists = isusage_score\
                      .join(package_classification, ['environment', 'package_code'], how='left')\
                      .filter(col('product') == self.product)\
                      .filter(col('environment').isin(self.environment))\
                      .filter(col('phase') == self.phase)\
                      .filter(col('reference_date') == reference_date)\
                      .orderBy(col('score').desc())\
                      .first()
        return downgrade_date_exists

    def lastUpdatedDate(self):
        activity_daily = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/ActivityDaily")
        y = activity_daily.where(col("activityid") == 1).select(fmax(col("date")).alias("date"))
        dsd = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/DivisionStatistics_DailyChanges")
        x = dsd.select(date_add(fmax(col("date")), -1).alias("date"))
        union_date = x.union(y).select(fmin(col("date")).alias("date"))

        return union_date

    def get_old_scores_downgrades(self, reference_date):
        isusage_score = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ISUsageScore/')\
                .select(trim(lower(col('environment'))).alias('environment'),
                        trim(lower(col('packagecode'))).alias('package_code'),
                        trim(lower(col('accountcode'))).cast(IntegerType()).alias('account_code'),
                        trim(lower(col('phase'))).alias('phase'),
                        col('score').cast(IntegerType()).alias('score'),
                        col('referencedate').cast(DateType()).alias('reference_date'),
                        col('predictdate').cast(DateType()).alias('predict_date'))

        package_class = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
            .select(trim(lower(col('environment'))).alias('environment'), 
                    trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('product'))).alias('product'))

        predict_date = isusage_score\
               .join(package_class, ['environment', 'package_code'], how='left')\
               .filter(col('product') == self.product)\
               .filter(col('predict_date') < reference_date)\
               .filter(col('environment').isin(self.environment))\
               .orderBy(col('predict_date').desc())\
               .select('predict_date')\
               .first()\
               .asDict()['predict_date']


        old_class = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
            .select(trim(lower(col('environment'))).alias('environment'),
                    trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('product'))).alias('product'))\
            .filter(col('product') == self.product)

        new_class = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('product'))).alias('product'))\
            .filter(col('product') == 'accounting')

        newcontracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'),
                    trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('eventdate'))).alias('event_date'),
                    trim(lower(col('itemtype'))).alias('item_type'),
                    trim(lower(col('inflowoutflow'))).alias('inflow_outflow'),
                    trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('eventtype'))).alias('event_type'))\
            .filter(col('environment') == 'nl')\
            .filter(col('event_type') == 'cdn')\
            .filter(col('item_type') == 'package')\
            .filter(col('inflow_outflow') == 'inflow')\
            .filter(col('event_date') > predict_date)\
            .filter(col('event_date') < predict_date + relativedelta(months=3))

        oldcontracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Contracts/')\
                .select(trim(lower(col('environment'))).alias('environment'),
                    trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('eventdate'))).alias('event_date'),
                    trim(lower(col('itemtype'))).alias('item_type'),
                    trim(lower(col('inflowoutflow'))).alias('inflow_outflow'),
                    trim(lower(col('eventtype'))).alias('event_type'))\
                .filter(col('inflow_outflow') == 'outflow')
        
        dg = newcontracts.alias('new_contracts')\
                    .join(oldcontracts.alias('old_contracts'), (col("new_contracts.account_code") == col("old_contracts.account_code")) &
                                                       (col("new_contracts.environment") == col("old_contracts.environment")) &
                                                       (col("new_contracts.event_date") == col("old_contracts.event_date")) &
                                                       (col("new_contracts.item_type") == col("old_contracts.item_type")) &
                                                       (col("new_contracts.event_type") == col("old_contracts.event_type"))
                      , how='inner')\
                    .join(new_class.alias('new_class'),(col("new_class.environment") == col("new_contracts.environment")) & (col("new_class.package_code") == col("new_contracts.package_code")), how='inner')\
                    .join(old_class.alias('old_class'), (col("old_class.environment") == col("old_contracts.environment")) & (col("old_class.package_code") == col("old_contracts.package_code")), how='inner')\
                    .select(col('new_contracts.environment').alias('environment'),
                            col('new_contracts.account_code').cast(IntegerType()).alias('account_code'),
                            col('new_contracts.event_type').alias('event_type'))
        
        results = isusage_score.alias('isus')\
            .join(package_class, ['environment', 'package_code'], how='left')\
            .join(dg,['account_code', 'environment'], how='left')\
            .filter(col('isus.predict_date').cast(DateType()) == predict_date)\
            .filter(col('product') == self.product)\
            .select('account_code', 'environment', 'phase', 'score', col('reference_date').alias('downgrade_date'), 'predict_date', 'product')
        return results
