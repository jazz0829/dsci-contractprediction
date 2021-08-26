from pyspark.sql.functions import trim, countDistinct, col, lower, upper, lit, first, when, row_number, log, datediff, date_add, greatest, coalesce
from pyspark.sql.functions import max as fmax, min as fmin, sum as fsum
from pyspark.sql import Window
from common.common_feature import CommonFeature

class AccountingOnboardingFeature(CommonFeature):

	def get_master_data_setup_type(self, referencedate):

		end_date = referencedate

		master_data_setup = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/Divisions_MasterDataSetup").alias("DMDS")
		divisions = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/Divisions")
		activity_daily_contracts = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/ActivityDaily_Contracts")
		package_classification = self.spark.read.parquet("s3://cig-prod-domain-bucket/Data/PackageClassification").where(col("product") == self.product)
		master_data_setup = master_data_setup.withColumn("divisioncode", trim(upper(col("divisioncode")))) \
        					.withColumn("environment", trim(upper(col("environment")))) \
                            			.where(~(col("masterdatasetuptype") == 'TemporaryDivision') \
                            			& (col("masterdatasetuptype").isNotNull()))

		divisions = divisions.withColumn("divisioncode", trim(upper(col("divisioncode")))) \
        					.withColumn("environment", trim(upper(col("environment"))))

        	join_master_on_division = master_data_setup.join(divisions, ["divisioncode", "environment"])

        	selected = join_master_on_division.select("environment", "divisioncode", "DMDS.accountid", "divisioncreated", "masterdatasetuptype",
                            			when(((col("masterdatasetuptype") == "Conversion") | (col("masterdatasetuptype") == "XML/CSV_Import")), '1') \
                            			.when(((col("masterdatasetuptype") == 'AccountantTemplate') | (col("masterdatasetuptype") == 'DivisionTransfer')), '2') \
                            			.when(((col("masterdatasetuptype") == 'ExactTemplate')), '3') \
                            			.when(((col("masterdatasetuptype") == 'Demo')), '4') \
                            			.when(((col("masterdatasetuptype") == 'DivisionCopy') | (col("masterdatasetuptype") == 'Manual') | (col("masterdatasetuptype") == 'Empty')), '5').alias("categoryrank"),
                            			when(((col("masterdatasetuptype") == "Conversion") | (col("masterdatasetuptype") == "XML/CSV_Import")), 'Other Software') \
                            			.when(((col("masterdatasetuptype") == 'AccountantTemplate') | (col("masterdatasetuptype") == 'DivisionTransfer')), 'Accountant Related') \
                            			.when(((col("masterdatasetuptype") == 'ExactTemplate')), 'Exact Template') \
                            			.when(((col("masterdatasetuptype") == 'Demo')), 'Demo') \
                            			.when(((col("masterdatasetuptype") == 'DivisionCopy') | (col("masterdatasetuptype") == 'Manual') | (col("masterdatasetuptype") == 'Empty')), 'Other').alias("category"),
                            			"masterdatasetupstatus", "eventstarttime").alias("SUB")

        	w_env_row = Window.partitionBy(col("accountid")).orderBy(col("SUB.categoryrank"), col("divisioncreated"))
		selected_with_window = selected.select("environment", "accountid", "divisioncreated", "masterdatasetuptype",
                                		"SUB.categoryrank", "SUB.category", "masterdatasetupstatus", "eventstarttime",
                                		row_number().over(w_env_row).alias("rownumber")).where((col("rownumber") == 1) 
                                		& (col("environment").isin(self.environment)))

        	w_env_mob = Window.partitionBy(col('environment'), col('accountcode')).orderBy(col('date').desc())

		activity_daily_contracts = activity_daily_contracts.withColumn("environment", trim(upper(col("environment")))) \
                 				.withColumn("accountid", trim(upper(col("accountid")))) \
                 				.withColumn("accountcode", trim(upper(col("accountcode")))) \
                 				.withColumn("packagecode", trim(upper(col("packagecode")))) \
                 				.where((col("date") <= end_date)).withColumn("datenumber", row_number().over(w_env_mob))\
                 				.where((col("datenumber") == 1)).alias("Contracts").select("environment", "accountcode", "packagecode", "datenumber", "accountid")

        	join_daily_on_sub1 = selected_with_window.join(activity_daily_contracts, ["accountid"], "left")

        	package_classification = package_classification.withColumn("packagecode", trim(upper(col("packagecode")))) \
                                               .withColumn("environment", trim(upper(col("environment"))))
        	join_on_package = join_daily_on_sub1.join(package_classification, ["packagecode", "environment"])
		
		return join_on_package.select(col("Contracts.environment"), col("accountcode").alias('account_code'), col("category").alias("master_data_setup_type"))
