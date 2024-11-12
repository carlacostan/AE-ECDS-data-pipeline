# Databricks notebook source
# Mount Blob Storage
storage_account_key = dbutils.secrets.get(scope="keys", key="storage_account_key")
dbutils.fs.unmount("/mnt/aedata")
dbutils.fs.mount(
    source = "wasbs://ecdsdata@aeecdsdata.blob.core.windows.net",
    mount_point = "/mnt/aedata",
    extra_configs = {"fs.azure.account.key.aeecdsdata.blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

# MAGIC %pip install openpyxl
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date, when, trim, year

# Retrieve key and container names from Databricks secrets
storage_account_key = dbutils.secrets.get(scope="keys", key="storage_account_key")
storage_account_name = dbutils.secrets.get(scope="keys", key="storage_account_name")
raw_data_container = dbutils.secrets.get(scope="keys", key="raw_data_container")
cleaned_data_container = dbutils.secrets.get(scope="keys", key="cleaned_data_container")

# Initialise Spark session
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Set up storage account credentials
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Define the path to the raw data in Azure Blob Storage
input_path = f"wasbs://{raw_data_container}@{storage_account_name}.blob.core.windows.net/AE_2324_ECDS_open_data_csv.csv"

# Load the raw data from Azure Blob Storage
raw_df = spark.read.option("header", "true").csv(input_path)

# Display the schema and a sample of data for validation
raw_df.printSchema()
raw_df.show(5)

# Data cleaning steps
# Ensure columns are trimmed and nulls are handled
raw_df = raw_df.select([trim(col(c)).alias(c) for c in raw_df.columns])

# Replace empty strings with nulls in MEASURE_VALUE
cleaned_df = raw_df.withColumn("MEASURE_VALUE", when(col("MEASURE_VALUE") == "", None).otherwise(col("MEASURE_VALUE")))

# Convert MEASURE_VALUE to a numeric format by removing commas and casting to integer
cleaned_df = cleaned_df.withColumn("MEASURE_VALUE", regexp_replace(col("MEASURE_VALUE"), ",", "").cast("int"))

# Standardize REPORTING_PERIOD and remove rows with unsupported formats
cleaned_df = cleaned_df.withColumn(
    "REPORTING_PERIOD",
    when(to_date(col("REPORTING_PERIOD"), "yyyy-MM").isNotNull(), to_date(col("REPORTING_PERIOD"), "yyyy-MM"))
    .when(to_date(col("REPORTING_PERIOD"), "yyyy/MM").isNotNull(), to_date(col("REPORTING_PERIOD"), "yyyy/MM"))
    .otherwise(None) 
)

# Filter out rows where REPORTING_PERIOD could not be converted or matches the unsupported yyyy/yy format
cleaned_df = cleaned_df.filter(col("REPORTING_PERIOD").isNotNull())

# Extract the year from REPORTING_PERIOD to create a new partition column
cleaned_df = cleaned_df.withColumn("year", year(col("REPORTING_PERIOD")))

# Display the number of rows and a sample of cleaned data
print(f"Number of rows after removing unsupported date formats: {cleaned_df.count()}")
cleaned_df.show(20)

# Write cleaned data to Data Lake with partitioning by year
output_path = f"abfss://{cleaned_data_container}@{storage_account_name}.dfs.core.windows.net/cleaned_AE_2324_ECDS"
cleaned_df.write.mode("overwrite").partitionBy("year").parquet(output_path)

