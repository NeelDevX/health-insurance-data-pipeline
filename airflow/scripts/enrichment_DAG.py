from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, current_timestamp, lit, upper, to_date, regexp_extract, lower, split, when

def create_spark_session():
    return SparkSession.builder \
        .appName("Healthcare Enrichment") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def enrich_bronze_table():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, trim, current_timestamp, lit, upper, to_date, regexp_extract, lower, split, when
    spark = create_spark_session()
    filename = Variable.get("json_filename")  # get from Airflow Variable
    hospital_name = filename.replace(".json", "").lower().replace(" ", "_").replace("-", "_")
    print(f"\n🚀 Enriching {hospital_name}")
    silver_name = f"local.silver.{hospital_name}"


    df_cleaned = spark.read.parquet(f"/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse/checkpoint_parquet/cleaning/")
    df_enriched = df_cleaned.withColumn("silver_ingestion_ts", current_timestamp())

    df_enriched = df_enriched.withColumn("payer_category", 
        when(col("payer_name").rlike("(?i)aetna|cigna|humana|anthem|imagine health|blue cross"), "Commercial")
        .when(col("payer_name").rlike("(?i)medicare|provider partners|devoted"), "Medicare")
        .when(col("payer_name").rlike("(?i)medicaid|community first|molina"), "Medicaid")
        .when(col("plan_name").rlike("(?i)HIX|exchange|blue advantage"), "Exchange")
        .otherwise("Other"))

    df_enriched = df_enriched.withColumn("pricing_model", 
        when(col("methodology").rlike("(?i)case rate"), "Case Rate")
        .when(col("methodology").rlike("(?i)fee schedule"), "Fee Schedule")
        .when(col("methodology").rlike("(?i)percent|percentage"), "Percentage-Based")
        .otherwise("Other"))

    df_enriched = df_enriched.withColumn("plan_type", 
        when(col("plan_name").rlike("(?i)hmo"), "HMO")
        .when(col("plan_name").rlike("(?i)ppo"), "PPO")
        .when(col("plan_name").rlike("(?i)hix|exchange"), "Exchange")
        .when(col("plan_name").rlike("(?i)medicare|medicaid"), "Government")
        .otherwise("Other"))

    df_enriched = df_enriched.withColumn("charge_bucket", 
        when(col("standard_charge_dollar") < 100, "Low")
        .when(col("standard_charge_dollar").between(100, 1000), "Medium")
        .when(col("standard_charge_dollar") > 1000, "High")
        .otherwise("Unknown"))
    
    # Calculate estimated_patient_amount
    df_enriched = df_enriched.withColumn(
        "estimated_patient_amount",
        col("gross_charge") - col("standard_charge_dollar")
    )

    df_enriched = df_enriched.withColumn("payer_info_missing", 
        when(col("payer_name").isNull() | col("plan_name").isNull() | col("methodology").isNull(), True)
        .otherwise(False))

    df_enriched = df_enriched.withColumn("treatment_type", 
        when(lower(col("service_description")).rlike("mri|ct|x-ray|ultrasound|imaging"), "Imaging")
        .when(lower(col("service_description")).rlike("injection|inj|tablet|tb|cp|oral|syrup|mg|solution|suspension"), "Medication")
        .when(lower(col("service_description")).rlike("biopsy|surgery|resection|repair|ablation|implant|arthroplasty|graft"), "Procedure")
        .when(lower(col("service_description")).rlike("panel|ab/|antibody|lab|urine|test|analysis|level|quant"), "Lab Test")
        .when(lower(col("service_description")).rlike("device|supply|graft|stent|pump|dressing"), "Supply/Device")
        .otherwise("Other"))

    df_enriched = df_enriched.withColumn("is_medication", 
        when(lower(col("service_description")).rlike("mg|tb|cp|solution|suspension|syrup|inhalation|injection"), True).otherwise(False))

    df_enriched = df_enriched.withColumn("drug_form", 
        when(lower(col("service_description")).rlike("tb|tablet|cp"), "Tablet")
        .when(lower(col("service_description")).rlike("inj|injection|ij"), "Injection")
        .when(lower(col("service_description")).rlike("sol|solution"), "Solution")
        .when(lower(col("service_description")).rlike("cream|ointment"), "Topical")
        .when(lower(col("service_description")).rlike("inhalation|ih|is"), "Inhaler")
        .otherwise("Other"))

    df_enriched = df_enriched.withColumn("imaging_type", 
        when(lower(col("service_description")).rlike("mri"), "MRI")
        .when(lower(col("service_description")).rlike("ct"), "CT Scan")
        .when(lower(col("service_description")).rlike("x-ray"), "X-Ray")
        .when(lower(col("service_description")).rlike("ultrasound"), "Ultrasound")
        .otherwise(None))

    df_enriched = df_enriched.withColumn("is_lab_test", 
        when(lower(col("service_description")).rlike("panel|antibody|test|level|urine|blood|cbc|cmp|lipid"), True).otherwise(False))

    df_enriched = df_enriched.withColumn("has_brand_indicator", 
        when(lower(col("service_description")).rlike("stryker|depuy|bard|zimmer|philips|medtronic|covidien|smith"), True).otherwise(False))

    df_address_split = df_enriched.withColumn("street", trim(split("hospital_address", ",")[0])) \
        .withColumn("city", trim(split("hospital_address", ",")[1])) \
        .withColumn("state_zip", trim(split("hospital_address", ",")[2]))

    df_address_cleaned = df_address_split \
        .withColumn("state", regexp_extract("state_zip", r"([A-Z]{2})", 1)) \
        .withColumn("zip_code", regexp_extract("state_zip", r"(\d{5})", 1)) \
        .drop("state_zip")

    spark.sql(f"DROP TABLE IF EXISTS {silver_name}")

    df_address_cleaned.writeTo(silver_name) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .createOrReplace()