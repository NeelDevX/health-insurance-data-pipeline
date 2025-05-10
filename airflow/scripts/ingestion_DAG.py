import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer
from airflow.models import Variable

raw_dir = "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse/raw_dataset/TX"

def create_spark_session():
    return SparkSession.builder \
        .appName("Healthcare JSON Ingestion") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def detect_and_flatten(df):
    # Check if top-level has 'pId' or 'provider_id' to identify provider
    id_col = "pId" if "pId" in df.columns else ("provider_id" if "provider_id" in df.columns else None)
    if not id_col:
        raise ValueError("❌ Cannot determine provider ID field. Not a supported JSON format.")

    # Try to access license info for schema-based detection
    try:
        license_fields = df.select("license_information.*").schema.names
    except:
        try:
            license_fields = df.selectExpr("root.license_information.*").schema.names
        except:
            raise ValueError("❌ Cannot access license_information for detection.")

    # Determine JSON type based on license key
    if "license" in license_fields:
        return flatten_type_1(df)
    elif "license_number" in license_fields:
        return flatten_type_2(df)
    else:
        raise ValueError("❌ Unknown license structure. Neither 'license' nor 'license_number' found.")

def flatten_type_1(df):
    #Explode standard_charge_information
    df_exploded = df.select(
        col("pId").alias("hospital_id"),
        col("hospital_name"),
        col("hospital_address")[0].alias("hospital_address"),
        col("hospital_location")[0].alias("hospital_location"),
        col("last_updated_on"),
        col("license_information.license").alias("license_number"),
        col("license_information.state").alias("license_state"),
        explode("standard_charge_information").alias("sci")
    )
    #Explode code_information
    df_code = df_exploded.select(
        "*",
        col("sci.description").alias("service_description"),
        explode("sci.code_information").alias("code_info"),
        col("sci.standard_charges").alias("charge_arr")
    )
    #Explode standard_charges
    df_charges = df_code.select(
        "*",
        col("code_info.code").alias("code"),
        col("code_info.modifiers").alias("modifiers"),
        col("code_info.type").alias("code_type"),
        explode("charge_arr").alias("charge")
    )
    #Explode payers_information
    df_flat = df_charges.select(
        "hospital_id", "hospital_name", "hospital_address", "hospital_location",
        "last_updated_on", "license_number", "license_state", "service_description",
        "code", "modifiers", "code_type",
        col("charge.setting").alias("care_setting"),
        col("charge.gross_charge"),
        col("charge.discounted_cash"),
        col("charge.minimum").alias("min_charge"),
        col("charge.maximum").alias("max_charge"),
        explode("charge.payers_information").alias("payer")
    ).select(
        "*",
        col("payer.payer_name"),
        col("payer.plan_name"),
        col("payer.billing_class"),
        col("payer.methodology"),
        col("payer.standard_charge_dollar"),
        col("payer.standard_charge_percentage"),
        col("payer.additional_payer_notes"),
        col("payer.standard_charge_algorithm")
    )
    return df_flat


def flatten_type_2(df):
    df_exp_1 = df.select(
        col("pId").alias("hospital_id"),
        col("hospital_name"),
        col("hospital_address")[0].alias("hospital_address"),
        col("hospital_location")[0].alias("hospital_location"),
        col("last_updated_on"),
        col("license_information.license_number").alias("license_number"),
        col("license_information.state").alias("license_state"),
        explode_outer("standard_charge_information").alias("sci")
    )
    # Step 3: Extract inside sci and explode code_information
    df_exp_2 = df_exp_1.select(
        "*",
        col("sci.description").alias("service_description"),
        col("sci.drug_information.type").alias("drug_type"),
        col("sci.drug_information.unit").alias("drug_unit"),
        explode_outer("sci.code_information").alias("code_info"),
        col("sci.standard_charges").alias("standard_charges")
    )
    # Step 4: Extract code and explode standard_charges
    df_exp_3 = df_exp_2.select(
        "*",
        col("code_info.code").alias("code"),
        col("code_info.modifiers").alias("modifiers"),
        col("code_info.type").alias("code_type"),
        explode_outer("standard_charges").alias("charge")
    )
    # Step 5: Extract final fields from exploded payer struct
    df_flat = df_exp_3.select(
        "hospital_id", "hospital_name", "hospital_address", "hospital_location", "last_updated_on",
        "license_number", "license_state", "service_description", "drug_type", "drug_unit",
        "code", "modifiers", "code_type",
        col("charge.setting").alias("care_setting"),
        col("charge.gross_charge"),
        col("charge.discounted_cash"),
        col("charge.minimum").alias("min_charge"),
        col("charge.maximum").alias("max_charge"),
        explode_outer("charge.payers_information").alias("payer")
    ).select(
        "hospital_id", "hospital_name", "hospital_address", "hospital_location", "last_updated_on",
        "license_number", "license_state", "service_description", "drug_type", "drug_unit",
        "code", "modifiers", "code_type", "care_setting", "gross_charge", "discounted_cash",
        "min_charge", "max_charge",
        col("payer.payer_name").alias("payer_name"),
        col("payer.plan_name").alias("plan_name"),
        col("payer.billing_class").alias("billing_class"),
        col("payer.methodology").alias("methodology"),
        col("payer.standard_charge_dollar").alias("standard_charge_dollar"),
        col("payer.standard_charge_percentage").alias("standard_charge_percentage"),
        col("payer.additional_payer_notes").alias("additional_payer_notes"),
        col("payer.pIdx").alias("payer_id")  # Keep only valid fields
    )
    return df_flat
    

def process_json_file():
    spark = create_spark_session()
    filename = Variable.get("json_filename")
    filepath = os.path.join(raw_dir, filename)
    
    df_raw = spark.read.option("multiline", "true").json(filepath)
    df_flat = detect_and_flatten(df_raw)

    hospital_name = filename.replace(".json", "").lower().replace(" ", "_").replace("-", "_")
    table_name = f"local.bronze.{hospital_name}"
    parquet_path = f"/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse/checkpoint_parquet/ingestion/{hospital_name}.parquet"

    table_df = spark.sql("SHOW TABLES IN local.bronze")
    existing_tables = [row['tableName'] for row in table_df.collect()]
    if hospital_name in existing_tables:
        raise ValueError(f"❌ Table '{table_name}' already exists skipping to prevent duplicate ingestion.")

    print(f"✅ Writing to Parquet format: {table_name}")
    df_flat.write.mode("overwrite").parquet(parquet_path)

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    df_back = spark.read.parquet(parquet_path)

    print(f"✅ Writing to iceberg table: {table_name}")
    df_back.writeTo(table_name) \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print(f"✅ Done: {table_name}")
    return True