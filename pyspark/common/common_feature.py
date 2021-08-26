"""" Common Features Extraction Object"""
from functools import reduce
from dateutil.relativedelta import relativedelta
from pyspark.sql import Window
from pyspark.sql.functions import trim, countDistinct, col, lower, lit, when, row_number, log, datediff, date_add, greatest, coalesce
from pyspark.sql.functions import max as fmax, sum as fsum, count as fcount
from pyspark.sql.types import DateType


class CommonFeature(object):
    """
    cf_obj = CommonFeature(spark, conf)
    reference_date = datetime.datetime(2019,1,1)
    ac = self.get_contract(reference_date)
    aa = self.get_active_accountant(ac, reference_date)
    pv = self.get_pageviews(aa, reference_date)
    df = self.get_divisionstatistics_financial(pv, reference_date)
    output_df = self.get_label(df, reference_date)
    """
    def __init__(self, spark, conf):
        """ initialization """
        self.spark = spark
        self.environment = conf.get('country').lower().split(',')
        self.product = conf.get('product').lower()
        self.window_contract = conf.get('window_contract')
        self.window_activities = conf.get('window_activities')
        self.unique_id = conf.get('unique_id')
        self.classification_codes = ['AC7', 'AC8', 'ACB', 'ACC', 'E04', 'EO1', 'EO2', 'EO3', 'EOL', 'EOR', 'EPR',
                                     'GR', 'KPN', 'KVK', 'NDA', 'PAR', 'AC1', 'ALL', 'CCB', 'INF', 'LAC', 'LEO',
                                     'REF', 'SC', 'UCM', 'UZ', 'VDA', 'WTC']
        self.label = conf.get('label')
        self.forcast_period = conf.get('period')

    def get_contract(self, reference_date):
        """ extract contracts """
        w_env_acc = Window.partitionBy('environment', 'account_code')
        c_ref_date = lit(reference_date).cast(DateType())
        had_trial = ((col('latest_trial_final_date') <= col('first_comm_start_date')) &\
                     (col('latest_trial_final_date') >= date_add(c_ref_date, -self.window_activities)))

        ordered_activity_daily_contract = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('packagecode'))).alias('package_code'), trim(lower(col('eventtype'))).alias('event_type'),
                    col('numberofusers').alias('number_of_users'), col('numberofadministrations').alias('number_of_administrations'),
                    col('accountantorlinked').alias('accountant_or_linked'), 'mrr', 'date')\
            .filter(col('date') <= reference_date)\
            .withColumn('last_contract_upgrade_date', fmax(when(col('event_type') == 'CUP', col('date')).otherwise(lit(None))).over(w_env_acc))\
            .withColumn('_rank', row_number().over(w_env_acc.orderBy(col('date').desc())))\
            .filter((col('_rank') <= 1) & col('environment').isin(self.environment))\
            .drop('_rank', 'date')

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('product'))).alias('product'), 'edition', 'solution')\
            .filter(col('product') == self.product)

        summary = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/AccountsContract_Summary/')\
            .filter((col('hadcommcontract') == 1) & (col('firstcommstartdate') < reference_date) & (col('latestcommfinaldate') > reference_date) &\
                    ((col('fullcancellationrequestdate') > reference_date) | col('fullcancellationrequestdate').isNull()))\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    col('firstcommstartdate').alias('first_comm_start_date'), col('latestcommfinaldate').alias('latest_comm_final_date'),
                    col('firsttrialstartdate').alias('first_trial_start_date'), col('latesttrialfinaldate').alias('latest_trial_final_date'),
                    col('fullcancellationrequestdate').alias('full_cancellation_request_date'))

        account_contract = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Accounts/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    col('name').alias('account_name'), col('isanonymized').alias('is_anonymized'), col('sectorcode').alias('sector_code'),
                    col('accountclassificationcode').alias('account_classification_code'))\
            .filter(col('accountclassificationcode').isin(self.classification_codes) & col('environment').isin(self.environment))\
            .join(ordered_activity_daily_contract, ['environment', 'account_code'], 'left')\
            .join(package_classification, ['environment', 'package_code'])\
            .join(summary, ['environment', 'account_code'])\
            .withColumn('tenure_in_months', datediff(c_ref_date, 'first_comm_start_date') / 30.4)\
            .filter(col('tenure_in_months') >= 6)\
            .withColumn('log_tenure_in_months', when(col('tenure_in_months') > 0, log(col('tenure_in_months')) + 1).otherwise(lit(0)))\
            .withColumn('last_contract_upgrade_date', coalesce('last_contract_upgrade_date', 'first_comm_start_date'))\
            .withColumn('tenure_upgrade_in_months', datediff('last_contract_upgrade_date', 'first_comm_start_date') / 30.4)\
            .withColumn('log_tenure_upgrade_in_months', when(col('tenure_upgrade_in_months') > 0, log(col('tenure_upgrade_in_months')) + 1).otherwise(lit(0)))\
            .withColumn('valid_contract_days', datediff(c_ref_date, greatest('first_comm_start_date', date_add(c_ref_date, -self.window_contract))) +\
                when(had_trial, datediff('latest_trial_final_date', greatest('first_trial_start_date',
                                                                             date_add(c_ref_date, -self.window_contract))) + 1).otherwise(0))\
            .withColumn('valid_activity_days', datediff(c_ref_date, greatest('first_comm_start_date', date_add(c_ref_date, -self.window_activities))) +\
                when(had_trial, datediff('latest_trial_final_date', greatest('first_trial_start_date',
                                                                             date_add(c_ref_date, -self.window_activities))) + 1).otherwise(0))\
            .select('environment', 'account_code', 'account_name', 'is_anonymized', 'account_classification_code', 'sector_code', 'event_type', 'package_code',
                    'number_of_users', 'number_of_administrations', 'accountant_or_linked', 'mrr', 'last_contract_upgrade_date', 'product', 'edition',
                    'solution', 'first_comm_start_date', 'latest_comm_final_date', 'first_trial_start_date', 'latest_trial_final_date',
                    'full_cancellation_request_date', 'tenure_in_months', 'tenure_upgrade_in_months', 'log_tenure_in_months',
                    'log_tenure_upgrade_in_months', 'valid_contract_days', 'valid_activity_days')
        return account_contract

    def get_active_accountant(self, input_df, reference_date):
        """ extract active accountants """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        accounts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Accounts/')\
            .select(col('isaccountant').alias('is_accountant'), lower(col('accountid')).alias('account_id'))

        users = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Users/')\
            .select(lower(col('userid')).alias('user_id'), lower(col('accountid')).alias('account_id'))\
            .join(accounts, ['account_id'])

        activity_daily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily/')\
            .select(lower(col('accountid')).alias('account_id'), trim(lower(col('accountcode'))).alias('account_code'),
                    trim(lower(col('environment'))).alias('environment'), lower(col('userid')).alias('user_id'),
                    trim(col('activityid')).alias('activity_id'), 'date', 'quantity')\
            .filter(col('environment').isin(self.environment))\
            .filter((col('date') >= start_date) & (col('date') <= end_date))\
            .filter(col('activity_id') == 1)

        is_entrepreneur = col('a.account_id') == col('u.account_id')
        is_accountant = (col('a.account_id') != col('u.account_id')) & (col('is_accountant') == 'Accountant')
        is_accountant_link_active = col('accountant_days_active') > 0
        is_accountant_link_inactive = (col('accountant_or_linked')) & (col('accountant_days_active') == 0)
        active_accountants = activity_daily.alias('a').join(users.alias('u'), ['user_id'])\
            .withColumn('user', when(is_entrepreneur, lit('Entrepreneur')).otherwise(when(is_accountant, lit('Accountant')).otherwise(lit('Other'))))\
            .select('a.environment', 'a.account_code', 'a.date', 'user')\
            .filter(col('user') != 'Other')\
            .groupby('environment', 'account_code', 'user')\
            .agg(countDistinct(when(col('user') == 'Entrepreneur', col('date')).otherwise(lit(None))).alias('entrepreneur_days_active'),
                 countDistinct(when(col('user') == 'Accountant', col('date')).otherwise(lit(None))).alias('accountant_days_active'))\
            .fillna({'entrepreneur_days_active': 0, 'accountant_days_active': 0})\
            .groupby('environment', 'account_code')\
            .agg(fmax('entrepreneur_days_active').alias('entrepreneur_days_active'), fmax('accountant_days_active').alias('accountant_days_active'))

        output = input_df.join(active_accountants, ['account_code', 'environment'], 'left')\
            .withColumn('accountant', when(is_accountant_link_active, lit('Active'))\
                .otherwise(when(is_accountant_link_inactive, lit('NotLinked')).otherwise(lit('NotLinked'))))\
            .withColumn('entrepreneur_active', (col('entrepreneur_days_active') > 0) & col('entrepreneur_days_active').isNotNull())
        return output

    def get_pageviews(self, input_df, reference_date):
        """ extract page views """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        # activity daily
        activity_daily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
        .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                trim(col('activityid')).alias('activity_id'), 'date', 'quantity')\
        .filter(col('environment').isin(self.environment))\
        .filter((col('date') >= start_date) & (col('date') <= end_date))\
        .filter(col('activity_id') == 1)\
        .drop('activity_id', 'date')

        # activity daily contracts
        w_env_acc = Window.partitionBy('environment', 'account_code')
        activity_daily_contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts')\
        .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                trim(lower(col('packagecode'))).alias('package_code'), 'date')\
        .filter(col('date') <= end_date)\
        .withColumn('_rank', row_number().over(w_env_acc.orderBy(col('date').desc())))\
        .filter(col('_rank') == 1)\
        .drop('date', '_rank')

        # package specifications
        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('product'))).alias('product'),
                    trim(lower(col('packagecode'))).alias('package_code'))\
            .filter(col('product') == self.product)\
            .drop('product')

        pageviews = activity_daily.join(activity_daily_contracts, ['environment', 'account_code'], 'inner')\
            .join(package_classification, ['environment', 'package_code'], 'inner')\
            .drop('package_code')\
            .groupBy(col('environment'), col('account_code'))\
            .agg(fsum('quantity').alias('pageviews'))\
            .withColumn('log_pageviews', when(col('pageviews') > 0, log(col('pageviews'))).otherwise(lit(0)))

        output = input_df.join(pageviews, ['account_code', 'environment'], 'left')
        return output

    def get_divisionstatistics_financial(self, input_df, reference_date):
        """ extract financial division statistics """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)

        w_env_div = Window.partitionBy(col('environment'), col('division_code')).orderBy(col('date').desc())
        divisionstatistics_daily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/DivisionStatistics_Daily')\
            .select(lower(trim(col('environment'))).alias('environment'), trim(col('divisioncode')).alias('division_code'),
                    col('abnamrobankaccounts').alias('abnamro_bank_accounts'), col('ingbankaccounts').alias('ing_bank_accounts'),
                    col('rabobankaccounts').alias('rabo_bank_accounts'), col('automaticbanklink').alias('automatic_bank_link'),
                    col('currencycount').alias('currency_count'), 'date')\
            .filter(col('date') <= end_date)\
            .filter(col('environment').isin(self.environment))\
            .withColumn('rn', row_number().over(w_env_div)).filter(col('rn') == 1)\
            .drop('rn')

        divisions = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Divisions')\
            .select(lower(trim(col('environment'))).alias('environment'), trim(col('divisioncode')).alias('division_code'),
                    lower(trim(col('accountid'))).alias('account_id'), 'deleted', 'blockingstatuscode')\
            .filter(col('blockingstatuscode') < 100)\
            .filter((col('deleted').isNull()) | (col('deleted') >= end_date))\
            .drop('blockingstatuscode', 'deleted')

        divisionstatistics_summary = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/DivisionStatistics_Summary')\
            .select(lower(trim(col('environment'))).alias('environment'), trim(col('divisioncode')).alias('division_code'),
                    col('salesinvoicefirstdate').alias('sales_invoice_first_date'))

        divisionstatistics_daily_change = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/DivisionStatistics_DailyChanges')\
            .select(lower(trim(col('environment'))).alias('environment'), trim(col('divisioncode')).alias('division_code'),
                    col('gltransactionscount').alias('gl_transactions_count'), col('banktransactions').alias('bank_transactions'),
                    col('bankentrycount').alias('bank_entry_count'), col('cashentrycount').alias('cash_entry_count'),
                    col('salesinvoicecount').alias('sales_invoice_count'), col('purchaseentrycount').alias('purchase_entry_count'),
                    col('stockcountentrycount').alias('stock_count_entry_count'), col('salesentrycount').alias('sales_entry_count'),
                    col('generaljournalentrycount').alias('general_journal_entry_count'), col('accountcount').alias('account_count'),
                    col('projecttotaltimeentries').alias('project_total_time_entries'), 'date')\
            .filter(col('environment').isin(self.environment))\
            .filter((col('date') >= start_date) & (col('date') <= end_date))\
            .drop('date')\
            .groupBy('environment', 'division_code')\
            .agg(fsum('gl_transactions_count').alias('sum_gl_transactions_count'), fsum('bank_transactions').alias('sum_bank_transactions'),
                 fsum('bank_entry_count').alias('sum_bank_entry_count'), fsum('cash_entry_count').alias('sum_cash_entry_count'),
                 fsum('sales_invoice_count').alias('sum_sales_invoice_count'), fsum('purchase_entry_count').alias('sum_purchase_entry_count'),
                 fsum('stock_count_entry_count').alias('sum_stock_count_entry_count'), fsum('sales_entry_count').alias('sum_sales_entry_count'),
                 fsum('general_journal_entry_count').alias('sum_general_journal_entry_count'), fsum('account_count').alias('sum_account_count'),
                 fsum('project_total_time_entries').alias('sum_project_total_time_entries'))

        w_env_acc = Window.partitionBy('environment', 'account_code').orderBy(col('date').desc())
        activity_daily_contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts')\
            .select(lower(trim(col('environment'))).alias('environment'), lower(trim(col('packagecode'))).alias('package_code'),
                    trim(col('accountcode')).alias('account_code'), lower(trim(col('accountid'))).alias('account_id'), 'date')\
            .filter(col('date') <= end_date)\
            .withColumn('_rank', row_number().over(w_env_acc))\
            .filter(col('_rank') == 1)\
            .drop('date', '_rank')

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification')\
            .select(lower(trim(col('environment'))).alias('environment'), lower(trim(col('packagecode'))).alias('package_code'),
                    lower(trim(col('product'))).alias('product'))\
            .filter(col('product') == self.product)\
            .drop('product')

        # join with divisionstatistics_summary
        sq_dss = divisionstatistics_daily.join(divisions, ['environment', 'division_code'], 'inner')\
            .join(divisionstatistics_summary, ['environment', 'division_code'], 'inner') # 516047 rows joined with 1727527 rows results 516047

        # join contracts and package classifications
        join_contracts_package = activity_daily_contracts.join(package_classification, ['environment', 'package_code'], 'inner')\
            .drop('package_code', 'environment') #results 194732

        # join sq_dss with contract packages and dsdc
        sq_dss_dsdc = sq_dss.join(join_contracts_package, ['account_id'], 'inner')\
            .join(divisionstatistics_daily_change, ['environment', 'division_code'], 'left') #results 100529

        # step1, find the distict divisions at each combination
        sq_dss_dsdc_divisions = sq_dss_dsdc\
            .groupBy('environment', 'account_code', 'account_id')\
            .agg(countDistinct('division_code').alias('num_activated_divisions'))

        # step2, find the other aggregations at each combination.
        is_big3_bank_account = when(col('abnamro_bank_accounts') > 0, lit(1))\
            .when(col('ing_bank_accounts') > 0, lit(1))\
            .when(col('rabo_bank_accounts') > 0, lit(1))\
            .otherwise(lit(0))
        sq_dss_dsdc_aggregates = sq_dss_dsdc\
            .withColumn('big3_bank_account', is_big3_bank_account)\
            .withColumn('ever_sent_invoice', when(col('sales_invoice_first_date') <= end_date, lit(1)).otherwise(lit(0)))\
            .groupBy('environment', 'account_code', 'account_id')\
            .agg(fsum('sum_gl_transactions_count').alias('gl_transactions'), fsum('sum_sales_invoice_count').alias('sales_invoice'),
                 fsum('sum_purchase_entry_count').alias('purchase_entry_count'), fsum('sum_stock_count_entry_count').alias('stock_count_entry'),
                 fsum('sum_bank_entry_count').alias('bank_entry_count'), fsum('sum_cash_entry_count').alias('cash_entry_count'),
                 fsum('sum_sales_entry_count').alias('sales_entry_count'), fsum('sum_general_journal_entry_count').alias('general_journal_entry_count'),
                 fsum('sum_account_count').alias('account_count'), fsum('sum_project_total_time_entries').alias('project_total_time_entries'),
                 fmax('big3_bank_account').alias('big3_bank_account'), fmax('automatic_bank_link').alias('bank_link'),
                 fmax('ever_sent_invoice').alias('ever_sent_invoice'), fmax('currency_count').alias('currency_count'))

        df_out = sq_dss_dsdc_aggregates.join(sq_dss_dsdc_divisions, ['environment', 'account_code', 'account_id'], 'inner')\
            .drop('account_id')

        # transformations
        bin_cols = ['big3_bank_account', 'bank_link', 'sales_invoice', 'currency_count']
        df_out = reduce(lambda y, x: y.withColumn('has_'+x, when((col(x) > 0) & col(x).isNotNull(), lit(1)).otherwise(lit(0))), bin_cols, df_out)

        int_cols = ['gl_Transactions', 'purchase_entry_count', 'bank_entry_count', 'sales_entry_count', 'account_count', 'cash_entry_count',
                    'general_journal_entry_count', 'num_activated_divisions']
        df = reduce(lambda y, x: y.withColumn('log_'+x, when(col(x) > 0, log(col(x))).otherwise(lit(0))), int_cols, df)
        return input_df.join(df, ['account_code', 'environment'], 'left')

    def get_active_days(self, reference_date):
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)
        w_env_acc =  Window.partitionBy('environment', 'accountcode')

        activity_daily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'), 
                col('date'), trim(lower(col('activityid'))).alias('activity_id'))
        
        contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'), 
                trim(lower(col('packagecode'))).alias('package_code'), col('date'))\
            .withColumn('date_number', row_number().over(w_env_acc.orderBy(col('date').desc())))\
            .filter(col('date') <= end_date)

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification/')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('packagecode'))).alias('package_code'),
                    trim(lower(col('product'))).alias('product'))

        active_days = activity_daily.alias('activity')\
                    .join(contracts, ['environment', 'account_code'], 'left')\
                    .join(package_classification.alias('packageclassification'), ['environment', 'package_code'], 'left')\
                    .filter(lower(col('environment')).isin(self.environment))\
                    .filter(col('activity.date') >= start_date)\
                    .filter(col('activity.date') <= end_date)\
                    .filter(col('date_number') == 1)\
                    .filter(col('product') == self.product)\
                    .filter(col('activity_id') == '2')\
                    .groupBy('environment', 'account_code').agg(fcount('activity.date').alias('active_day'))\
                    .select(col('environment'), col('account_code'), col('active_day'))
        
        return active_days

    def get_mobile(self, input_df, reference_date):
        """ get mobile features """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)
        activity_daily = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily')
        activity_daily = activity_daily.withColumn("environment", trim(lower(col("environment")))) \
                                .withColumn("accountcode", trim(lower(col("accountcode")))) \
                                .withColumnRenamed('accountcode', 'account_code')\
                                .where((col("activityid") == 8) & (col("date") >= start_date) \
                                & (col("date") <= end_date) & (col("environment").isin(self.environment)))

        activity_daily_contracts = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts')
        w_env_mob = Window.partitionBy(col('environment'), col('account_code')).orderBy(col('date').desc())
        activity_daily_contracts = activity_daily_contracts.withColumn("environment", trim(lower(col("environment")))) \
                 .withColumn("account_code", trim(lower(col("accountcode")))) \
                 .withColumn("package_code", trim(lower(col("packagecode")))) \
                 .where((col("date") <= end_date)).withColumn("datenumber", row_number().over(w_env_mob))\
                 .where((col("datenumber") == 1)).select("environment", "account_code", "package_code", "datenumber")

        package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification')\
                .where(col("product") == self.product)

        package_classification = package_classification.withColumn("environment", trim(lower(col("environment")))) \
                 .withColumn("packagecode", trim(lower(col("packagecode"))))\
                 .withColumnRenamed('accountcode', 'account_code')\
                 .withColumnRenamed('packagecode', 'package_code')\

        join_daily_with_contracts = activity_daily.join(activity_daily_contracts, ["environment", "account_code"], "left")
        join_on_package = join_daily_with_contracts.join(package_classification, ["environment", "package_code"])
        grouped_mobile = join_on_package.groupby(['account_code', 'environment']).agg(fsum(col("quantity")).alias("use_mobile"))\
                .withColumnRenamed('accountid', 'account_id')\
                .withColumnRenamed('divisioncode', 'division_code')\
                .withColumn('recent_use_mobile', col('use_mobile').isNotNull())

        return input_df.join(grouped_mobile, ['account_code', 'environment'], 'left')

    def get_downgrade(self, input_df, reference_date):
        """ get downgrade features """
        end_date = reference_date
        start_date = end_date - relativedelta(days=self.window_activities)
        #read parquet
        contract = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/Contracts')\
            .select(trim(lower(col('environment'))).alias('environment'), trim(lower(col('accountcode'))).alias('account_code'),
                    col('eventdate').alias('event_date'), trim(lower(col('itemtype'))).alias('item_type'), trim(lower(col('eventtype'))).alias('event_type'),
                    trim(lower(col('packagecode'))).alias('package_code'), trim(lower(col('inflowoutflow'))).alias('inflow_outflow'))
        contract = contract.withColumn('event_date', col('event_date').cast(DateType()))
        new_package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification')\
            .select(trim(lower(col('environment'))).alias('environment'), col('packagecode').alias('package_code'), col('product'))\
            .filter(col('Product') == 'Accounting')
        new_contract = contract.alias('nc').join(new_package_classification.alias('npc'), ['environment', 'package_code'], 'left')\
            .filter(col('inflow_outflow') == 'inflow')
        old_package_classification = self.spark.read.parquet('s3://cig-prod-domain-bucket/Data/PackageClassification')\
            .select(trim(lower(col('environment'))).alias('environment'), col('packagecode').alias('package_code'), col('product'))\
            .filter(col('Product') == self.product)
        old_contract = contract.alias('oc').join(old_package_classification.alias('opc'), ['environment', 'package_code'], 'left')\
            .filter(col('inflow_outflow') == 'outflow')
        #query
        join_columns = ['account_code', 'environment', 'event_date', 'item_type', 'event_type']
        contract_downgrade = new_contract.alias('new').join(old_contract.alias('old'), join_columns, 'left')\
            .select(col('new.environment').alias('environment'),
                    col('new.account_code').alias('account_code'),
                    col('new.event_date').alias('event_date'))\
            .distict()\
            .filter(col('new.event_type') == 'cdn')\
            .filter(col('new.item_type') == 'package')\
            .filter(col('new.event_date') >= start_date)\
            .filter(col('new.event_date') <= end_date)
        return input_df.join(contract_downgrade, ['account_code', 'environment'], 'left')

    def get_label(self, input_df, reference_date):
        """ get labels"""
        forcast_date = reference_date + relativedelta(months=self.forcast_period)
        if self.label == 'churned':
            is_churned = (col('full_cancellation_request_date') <= forcast_date) | (col('latest_comm_final_date') <= forcast_date)
            output_df = input_df.withColumn('churned', when(is_churned, lit(1)).otherwise(lit(0)))
        elif self.label == 'downgraded':
            output_df = self.get_downgrade(input_df, reference_date)\
                .withColumn('downgraded', when(col('downgrade_date').isNotNull(), lit(1)).otherwise(lit(0)))
        return output_df

    def get_window_data(self, reference_date):
        """ extract features dummy """
        print(reference_date)

    def get_dataset(self, current_date, mode):
        """ concatenate dataset based on number of windows"""
        if mode == 'validate':
            pass
        else:
            if mode == 'train':
                num_window = 4
                shifted_window = [current_date - relativedelta(days=self.window_contract*i) for i in range(1, num_window+1)]
            elif mode == 'predict':
                shifted_window = [current_date]
            elif mode == 'evaluate':
                shifted_window = [current_date - relativedelta(days=self.window_contract*1.5)]
            df_list = [self.get_window_data(reference_date) for reference_date in shifted_window]
            df_result = reduce(lambda df_result, df_dataset: df_result.union(df_dataset), df_list)
            return df_result
