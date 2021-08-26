# -*- coding: utf-8 -*-

"""
get WholesaleDistributionFeature features
- Author: Estelle Rambier
-------------------------
"""

from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F
from pyspark.sql import Window
from common.common_feature import CommonFeature

class WholesaleDistributionFeature(CommonFeature):
    """ wholesale distribution feature """
    def __init__(self, spark, conf, reference_date, path='s3://cig-prod-domain-bucket/Data/'):
        super(WholesaleDistributionFeature, self).__init__(spark, conf)
        self.reference_date = reference_date
        self.features = self.spark.createDataFrame([(1, 2)], ['environment', 'accountcode'])# init
        self.filepath = path
        self.select_columns = list(set(conf.get('predictors') + [self.label] + conf.get('unique_id')))

    def get_ds_wd(self, input_df):
        """ get wholesale features """
        window_first = Window.partitionBy(F.col('environment'), F.col('divisioncode')).orderBy(F.col('date'))
        window_last = Window.partitionBy(F.col('environment'), F.col('divisioncode')).orderBy(F.col('date').desc())
        window_cont = Window.partitionBy(F.col('environment'), F.col('accountcode')).orderBy(F.col('date').desc())
        col_div_stat = ['environment', 'divisioncode', 'date', 'quotecount', 'customercount', 'suppliercount', 'salesinvoicecount', 'stockcountentrycount',
                        'salesorderentrycount', 'purchaseorderentrycount', 'deliveryentrycount', 'itemcount', 'salespricelistcount']
        col_acc_daily = ['environment', 'accountcode', 'packagecode', 'accountid', 'date']
        int_cols = ["quote_count", "customer_count", "supplier_count", "sales_invoice_count", "stock_count_entry_count", "sales_order_entry_count",
                    "purchase_order_entry_count", "delivery_entry_count", "item_count", "sales_price_list_count"]

        environment = [x.lower() for x in self.environment]
        product = self.product
        end_date = self.reference_date
        start_date = end_date - relativedelta(days=self.window_activities)
        path = self.filepath
        spark = self.spark
        # READINGS
        div_stat_daily = spark.read.parquet(path + 'DivisionStatistics_Daily/*/*/*/*.parquet')\
        .filter(F.col('date').between(start_date, end_date))\
        .select(col_div_stat)\
        .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
        .fillna(0)

        first_rec = div_stat_daily.withColumn('rn_first', F.row_number().over(window_first))\
        .withColumnRenamed('quotecount', 'first_record_quotecount')\
        .withColumnRenamed('customercount', 'first_record_customercount')\
        .withColumnRenamed('suppliercount', 'first_record_suppliercount')\
        .withColumnRenamed('salesinvoicecount', 'first_record_salesinvoicecount')\
        .withColumnRenamed('stockcountentrycount', 'first_record_stockcountentrycount')\
        .withColumnRenamed('salesorderentrycount', 'first_record_salesorderentrycount')\
        .withColumnRenamed('purchaseorderentrycount', 'first_record_purchaseorderentrycount')\
        .withColumnRenamed('deliveryentrycount', 'first_record_deliveryentrycount')\
        .withColumnRenamed('itemcount', 'first_record_item_count')\
        .withColumnRenamed('salespricelistcount', 'first_record_salespricelistcount')\
        .filter(F.col('rn_first') == 1)

        last_rec = div_stat_daily.withColumn('rn_Last', F.row_number().over(window_last))\
        .withColumnRenamed('quotecount', 'last_record_quotecount')\
        .withColumnRenamed('customercount', 'last_record_customercount')\
        .withColumnRenamed('suppliercount', 'last_record_suppliercount')\
        .withColumnRenamed('salesinvoicecount', 'last_record_salesinvoicecount')\
        .withColumnRenamed('stockcountentrycount', 'last_record_stockcountentrycount')\
        .withColumnRenamed('salesorderentrycount', 'last_record_salesorderentrycount')\
        .withColumnRenamed('purchaseorderentrycount', 'last_record_purchaseorderentrycount')\
        .withColumnRenamed('deliveryentrycount', 'last_record_deliveryentrycount')\
        .withColumnRenamed('itemcount', 'last_record_item_count')\
        .withColumnRenamed('salespricelistcount', 'last_record_salespricelistcount')\
        .filter(F.col('rn_Last') == 1)

        divisions_preproc = spark.read.parquet(path + 'Divisions/*.parquet')\
        .filter(F.col('blockingstatuscode') < 100)\
        .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
        .filter((F.col('deleted') > end_date) | (F.col('deleted').isNull()))\
        .filter(F.col('environment').isin(environment))\
        .withColumn('accountid', F.lower(F.regexp_replace(F.col("accountid"), " ", "")))

        actdaily_contracts = spark.read.parquet(path + 'ActivityDaily_Contracts/*.parquet')\
        .withColumn('accountid', F.lower(F.regexp_replace(F.col("accountid"), " ", "")))\
        .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
        .select(col_acc_daily)\
        .filter(F.col('date') <= end_date)\
        .withColumn('dateNumber', F.row_number().over(window_cont))\
        .filter(F.col('dateNumber') == 1)

        package_classif = spark.read.parquet(path + 'PackageClassification/*.parquet')\
        .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
        .withColumn('packagecode', F.regexp_replace(F.col("packagecode"), " ", ""))\
        .withColumn('packagecode', F.lower(F.col('packagecode')))\
        .filter(F.lower(F.col('product')) == product.lower())

        # JOINS
        data_join = first_rec.join(last_rec, ['environment', 'divisioncode'], how='inner')\
        .join(divisions_preproc, on=['divisioncode', 'environment'], how='inner')\
        .join(actdaily_contracts, on=['accountid', 'environment'], how='inner')\
        .join(package_classif, on=['environment', 'packagecode'], how='inner')

        # FEATURE ENGINERRING
        final = data_join.withColumn('account_code', F.regexp_replace(F.col("accountcode"), " ", ""))\
        .withColumn('environment', F.lower((F.regexp_replace(F.col("environment"), " ", ""))))\
        .withColumn('quotecount', F.col("last_record_quotecount") - F.col("first_record_quotecount"))\
        .withColumn('customercount', F.col("last_record_customercount") - F.col("first_record_customercount"))\
        .withColumn('suppliercount', F.col("last_record_suppliercount") - F.col("first_record_suppliercount"))\
        .withColumn('salesinvoicecount', F.col("last_record_customercount") - F.col("first_record_customercount"))\
        .withColumn('stockcountentrycount', F.col("last_record_stockcountentrycount") - F.col("first_record_stockcountentrycount"))\
        .withColumn('salesorderentrycount', F.col("last_record_salesorderentrycount") - F.col("first_record_salesorderentrycount"))\
        .withColumn('purchaseorderentrycount', F.col("last_record_purchaseorderentrycount") - F.col("first_record_purchaseorderentrycount"))\
        .withColumn('deliveryentrycount', F.col("last_record_deliveryentrycount") - F.col("first_record_deliveryentrycount"))\
        .withColumn('item_count', F.col("last_record_item_count") - F.col("first_record_item_count"))\
        .withColumn('salespricelistcount', F.col("last_record_salespricelistcount") - F.col("first_record_salespricelistcount"))\
        .groupBy(['accountcode', 'environment'])\
        .agg({'quotecount':"sum", 'customercount':"sum",
              'suppliercount':'sum', 'salesinvoicecount':'sum',
              'stockcountentrycount':'sum', 'salesorderentrycount':'sum',
              'purchaseorderentrycount':'sum', 'deliveryentrycount':'sum',
              'item_count':'sum', 'salespricelistcount':'sum'})\
        .withColumnRenamed("SUM(quotecount)", "quote_count")\
        .withColumnRenamed("SUM(customercount)", "customer_count")\
        .withColumnRenamed("SUM(suppliercount)", "supplier_count")\
        .withColumnRenamed("SUM(salesinvoicecount)", "sales_invoice_count")\
        .withColumnRenamed("SUM(stockcountentrycount)", "stock_count_entry_count")\
        .withColumnRenamed("SUM(salesorderentrycount)", "sales_order_entry_count")\
        .withColumnRenamed("SUM(purchaseorderentrycount)", "purchase_order_entry_count")\
        .withColumnRenamed("SUM(deliveryentrycount)", "delivery_entry_count")\
        .withColumnRenamed("SUM(item_count)", "item_count")\
        .withColumnRenamed("SUM(salespricelistcount)", "sales_price_list_count")\
        .withColumnRenamed("accountcode", "account_code")\
        .select(*[[F.log(F.col(x)).alias('log_'+x) for x in int_cols]+ ['environment', 'account_code']])\
        .join(input_df, how='left', on=['environment', 'account_code'])

        self.features = final
        return final

    def get_active_user_divisions(self, input_df, reference_date):
        """ get active user divisions for wholesale """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        activitydaily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
                         .select('date', F.col('divisioncode').alias('division_code'),
                                 F.trim(F.lower(F.col('activityid'))).alias('activity_id'),
                                 F.trim(F.lower(F.col('userid'))).alias('user_id'),
                                 F.trim(F.lower(F.col('accountcode'))).alias('account_code'),
                                 F.trim(F.lower(F.col('environment'))).alias('environment'))\
                         .select('environment', 'account_code', 'user_id', 'division_code', 'date', 'activity_id')\
                         .filter((F.col('date') >= start_date) & (F.col('date') <= end_date))\
                         .filter(F.col('activity_id') == 1)

        w_env_div = Window.partitionBy(F.col('environment'), F.col('account_code')).orderBy(F.col('date').desc())
        contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
                    .select(F.trim(F.lower(F.col('environment'))).alias('environment'),
                            F.trim(F.lower(F.col('accountcode'))).alias('account_code'),
                            F.trim(F.lower(F.col('packagecode'))).alias('package_code'), 'date')\
                    .filter(F.col('date') <= end_date)\
                    .withColumn('date_number', F.row_number().over(w_env_div))

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
                         .select(F.trim(F.lower(F.col('product'))).alias('product'),
                                 F.trim(F.lower(F.col('environment'))).alias('environment'),
                                 F.trim(F.lower(F.col('packagecode'))).alias('package_code'))\
                         .filter(F.col('product') == self.product)

        active_users_division = activitydaily\
         .join(contracts, ['environment', 'account_code'], how='left')\
         .join(package_classification, ['environment', 'package_code'], how='left')\
         .filter(F.col('environment').isin(self.environment))\
         .filter(F.col('date_number') == 1)\
         .groupby(['environment', 'account_code'])\
         .agg(F.countDistinct('user_id').alias('active_users'),
              F.countDistinct('division_code').alias('active_divisions'))
        return active_users_division.join(input_df, how='left', on=['environment', 'account_code'])

    def get_sector_haswholesale(self, input_df):
        """ get sector info for wholesale"""
        word_list = "handel|trad|internation|product|materieel|wholesale"
        ouput_df = input_df.withColumn('sector', F.when(F.col('sector_code') == 'G', 'G').otherwise(
            F.when(F.col('sector_code') != 'G', 'non_g').otherwise('unknown')))\
            .withColumn('account_name', F.lower(F.col('account_name')))\
            .withColumn('has_wholesale_name', F.when(F.col('account_name').rlike('(^|\s)(' + word_list + ')(\s|$)'), 'True').otherwise('False'))
        return ouput_df

    def get_window_data(self, reference_date):
        """ extract features for wholesale distribution """
        ac_out = self.get_contract(reference_date)
        # TODO missing active days here (Chon Sin)
        df_out = self.get_divisionstatistics_financial(ac_out, reference_date)
        mo_out = self.get_mobile(df_out, reference_date)
        la_out = self.get_label(mo_out, reference_date)
        wd_out = self.get_sector_haswholesale(la_out)
        ds_out = self.get_ds_wd(wd_out)
        au_out = self.get_active_user_divisions(ds_out, reference_date)
        return au_out.select(self.select_columns)
