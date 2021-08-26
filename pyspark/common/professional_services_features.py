# -*- coding: utf-8 -*-

"""
get ProfessionalServicesFeature features
- Author: Estelle Rambier
-------------------------
"""

import datetime
import pyspark
import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import logging
from common.common_feature import CommonFeature


class ProfessionalServicesFeature(CommonFeature):
    def __init__(self, spark, conf, reference_date, path='s3://cig-prod-domain-bucket/Data/'):
        super(ProfessionalServicesFeature, self).__init__(spark, conf)
        self.reference_date = reference_date
        self.features = self.spark.createDataFrame([(1,2)], ['environment', 'accountcode'])# init
        self.filepath = path
        self.logger = logging.getLogger(__name__)

     def get_ds_psa(self):
        print('Getting Division Statistics Professional services...')
        PATH = self.filepath
        WINDOW = self.window_activities
        ENVIRONMENT = [x.lower() for x in self.environment]
        print(ENVIRONMENT)
        PRODUCT = self.product.lower()
        END_DATE = self.reference_date
        START_DATE = END_DATE - relativedelta(days=WINDOW)
        WINDOW_CONT = Window.partitionBy(F.col('environment'), F.col('accountcode')).orderBy(F.col('date').desc())
        COL_ACC_DAILY = ['environment', 'accountcode', 'packagecode', 'accountid', 'date']
        SPARK = self.spark
        print(PRODUCT)

        # FILTERS
        div_stat_daily = SPARK.read.option("mergeSchema", "true").parquet(*[PATH + 'DivisionStatistics_DailyChanges'])\
                .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
                .filter(F.col('date').between(START_DATE, END_DATE))

        divisions_preproc = SPARK.read.parquet(PATH + 'Divisions/*.parquet')\
                .filter(F.col('blockingstatuscode')< 100)\
                .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
                .filter((F.col('deleted') > END_DATE) | (F.col('deleted').isNull()))\
                .filter(F.col('environment').isin(ENVIRONMENT))\
                .withColumn('accountid',F.lower(F.regexp_replace(F.col("accountid"), " ", "")))

        actdaily_contracts = SPARK.read.parquet(PATH + 'ActivityDaily_Contracts/*.parquet')\
                .select(COL_ACC_DAILY)\
                .withColumn('accountid',F.lower(F.regexp_replace(F.col("accountid"), " ", "")))\
                .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
                .filter(F.col('date')<= END_DATE)\
                .withColumn('dateNumber', F.row_number().over(WINDOW_CONT))\
                .withColumn('packagecode', F.lower(F.regexp_replace(F.col("packagecode"), " ", "")))\
                .filter(F.col('dateNumber')==1)

        package_classif = SPARK.read.parquet(PATH + 'PackageClassification/*.parquet')\
                .withColumn('environment', F.lower(F.regexp_replace(F.col("environment"), " ", "")))\
                .withColumn('packagecode', F.lower(F.regexp_replace(F.col("packagecode"), " ", "")))\
                .filter(F.lower(F.regexp_replace(F.col("product"), " ", "")) == PRODUCT.lower().replace(" ", ""))

        # JOIN AND ENGINEERING
        final = div_stat_daily.join(divisions_preproc, how='inner', on=["environment","DivisionCode"])\
                .join(actdaily_contracts, how='inner', on=['accountid', 'environment'])\
                .join(package_classif, how='inner', on=['environment', 'packagecode'])\
                .withColumn('accountcode', F.regexp_replace(F.col("accountcode"), " ", ""))\
                .withColumn('environment', F.regexp_replace(F.col("environment"), " ", ""))\
                .groupBy(['accountcode', 'environment'])\
                .agg({'projecttotalcostentries':"sum", 'projecttypefixedprice':"sum",
                    'projecttypetimeandmaterial':'sum', 'projecttypenonbillable':'sum', 
                    'projecttypeprepaidretainer':'sum', 'projecttypeprepaidhtb':'sum'})\
                .withColumnRenamed("SUM(projecttotalcostentries)", "projecttotalcostentriesCount")\
                .withColumnRenamed("SUM(projecttypefixedprice)", "projecttypefixedprice")\
                .withColumnRenamed("SUM(projecttypetimeandmaterial)", "projecttypetimeandmaterial")\
                .withColumnRenamed("SUM(projecttypenonbillable)", "projecttypenonbillable")\
                .withColumnRenamed("SUM(projecttypeprepaidretainer)", "projecttypeprepaidretainer")\
                .withColumnRenamed("SUM(projecttypeprepaidhtb)", "projecttypeprepaidhtb")\
                .join(self.features, on=['environment', 'accountcode'], how='left')
        self.features = final

    def get_professional_services_feature(self, cf_obj): 
        # contract
        cf_obj.get_contract()
        # active days
        cf_obj.get_active_days()       
        # division statistics financial
        cf_obj.get_divisionstatistics_financial()    
        cf_obj.get_mobile()
        cf_obj.get_label()
        
        data = cf_obj.features.copy()
        data.SectorCode = data.SectorCode.fillna('Unknown')
        data.UseMobile = data.UseMobile.fillna(0)
        data.NumActivatedDivisions = data.NumActivatedDivisions.fillna(0)
        data.Big3BankAccount = data.Big3BankAccount.fillna(0)
        data.BankLink = data.BankLink.fillna(0)
        data.EverSentInvoice = data.EverSentInvoice.fillna(0)
        data.CurrencyCount = data.CurrencyCount.fillna(0)
        data.GLTransactions = data.GLTransactions.fillna(0)
        data.SalesInvoice = data.SalesInvoice.fillna(0)
        data.PurchaseEntryCount = data.PurchaseEntryCount.fillna(0)
        data.StockCountEntry = data.StockCountEntry.fillna(0)
        data.BankEntryCount = data.BankEntryCount.fillna(0)
        data.CashEntryCount = data.CashEntryCount.fillna(0)
        data.SalesEntryCount = data.SalesEntryCount.fillna(0)
        data.GeneralJournalEntryCount = data.GeneralJournalEntryCount.fillna(0)
        data.AccountCount = data.AccountCount.fillna(0)
        self.features = data
        
        # division statistics professional services
        self.get_ds_psa()

        return self.features
        
    
