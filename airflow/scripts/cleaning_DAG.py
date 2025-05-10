from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, to_date, current_timestamp

def create_spark_session():
    return SparkSession.builder \
        .appName("Healthcare Cleaning") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def clean_bronze_table():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, upper, to_date, current_timestamp
    spark = create_spark_session()
    filename = Variable.get("json_filename")  # get from Airflow Variable
    hospital_name = filename.replace(".json", "").lower().replace(" ", "_").replace("-", "_")
    print(f"\n🚀 Cleaning {hospital_name}")
    
    bronze_table = f"local.bronze.{hospital_name}"
    df = spark.read.format("iceberg").load(bronze_table)

    df_clean = (
        df.dropna(subset=["hospital_id", "hospital_name", "service_description", "gross_charge", "standard_charge_dollar"])
          .dropDuplicates()
          .withColumn("payer_name", upper(trim(col("payer_name"))))
          .withColumn("plan_name", upper(trim(col("plan_name"))))
          .withColumn("last_updated_on", to_date("last_updated_on", "yyyy-MM-dd"))
          .filter((col("gross_charge") > 0) & 
              (col("min_charge") <= col("max_charge")) &
              (col("gross_charge") >= col("standard_charge_dollar")))
    )

    parquet_path = f"/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse/checkpoint_parquet/cleaning/"
    print(f"✅ Writing cleaned data to Parquet: {parquet_path}")
    df_clean.write.mode("overwrite").parquet(parquet_path)

    print("🎉 Cleaning complete.")
