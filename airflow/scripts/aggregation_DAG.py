from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import monotonically_increasing_id, col

# Create Spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("Healthcare Aggregation") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType


def get_or_create_dim_tables(spark, silver_df):
    # Create new distinct payer names from current file
    payer_dim_new = silver_df.select("payer_name").dropDuplicates()

    try:
        existing_payers = spark.read.table("local.gold.dim_payer")
        max_payer_id = existing_payers.agg({"payer_id": "max"}).collect()[0][0] or 0
    except:
        payer_schema = StructType([
            StructField("payer_id", LongType(), True),
            StructField("payer_name", StringType(), True)
        ])
        existing_payers = spark.createDataFrame(spark.sparkContext.emptyRDD(), payer_schema)
        max_payer_id = 0

    payers_to_add = payer_dim_new.join(existing_payers, on="payer_name", how="left_anti") \
        .withColumn("payer_id", row_number().over(Window.orderBy("payer_name")) + max_payer_id) \
        .select("payer_id", "payer_name")

    payer_dim = existing_payers.unionByName(payers_to_add)

    # Create new distinct plans from current file
    plan_dim_new = silver_df.select("plan_name").dropDuplicates().na.drop(subset=["plan_name"])

    try:
        existing_plans = spark.read.table("local.gold.dim_plan")
        max_plan_id = existing_plans.agg({"plan_id": "max"}).collect()[0][0] or 0
    except:
        plan_schema = StructType([
            StructField("plan_id", LongType(), True),
            StructField("plan_name", StringType(), True)
        ])
        existing_plans = spark.createDataFrame(spark.sparkContext.emptyRDD(), plan_schema)
        max_plan_id = 0

    plans_to_add = plan_dim_new.join(existing_plans, on="plan_name", how="left_anti") \
        .withColumn("plan_id", row_number().over(Window.orderBy("plan_name")) + max_plan_id) \
        .select("plan_id", "plan_name")

    plan_dim = existing_plans.unionByName(plans_to_add)

    return payer_dim, plan_dim

def aggregate_gold_layer():
    spark = create_spark_session()

    filename = Variable.get("json_filename")
    hospital_name = filename.replace(".json", "").lower().replace(" ", "_").replace("-", "_")
    print(f"\n📦 Aggregating for {hospital_name}")

    silver_df = spark.read.table(f"local.silver.{hospital_name}")
    current_ts = datetime.now()
    silver_df = silver_df.withColumn("gold_ingestion_ts", F.lit(current_ts))

    hospital_dim = silver_df.select("hospital_id", "hospital_name", "city", "state", "license_number", "license_state").dropDuplicates()
    payer_dim, plan_dim = get_or_create_dim_tables(spark, silver_df)
    procedure_dim = silver_df.select("code", "modifiers", "code_type", "care_setting", "treatment_type", "is_medication", "drug_form", "imaging_type", "is_lab_test", "has_brand_indicator").dropDuplicates()
    fact_charges = silver_df.alias("s") \
        .join(payer_dim.alias("p"), on="payer_name", how="left") \
        .join(plan_dim.alias("pl"), on="plan_name", how="left") \
        .select(
            col("s.hospital_id"),
            col("p.payer_id"),
            col("pl.plan_id"),
            col("s.code"),
            col("s.gross_charge"),
            col("s.discounted_cash"),
            col("s.min_charge"),
            col("s.max_charge"),
            col("s.estimated_patient_amount"),
            col("s.standard_charge_dollar"),
            col("s.standard_charge_percentage"),
            col("s.charge_bucket"),
            col("s.payer_info_missing"),
            col("s.silver_ingestion_ts"),
            col("s.gold_ingestion_ts")
        )
    
    gold_tables = [
        (hospital_dim, "local.gold.dim_hospital"),
        (payer_dim, "local.gold.dim_payer"),
        (plan_dim, "local.gold.dim_plan"),
        (procedure_dim, "local.gold.dim_procedure"),
        (fact_charges, "local.gold.fact_charges")
    ]

    for df, table in gold_tables:
        try:
            df.writeTo(table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .append()
            print(f"✅ Appended to existing table: {table}")
        except AnalysisException:
            df.writeTo(table) \
                .using("iceberg") \
                .tableProperty("format-version", "2") \
                .create()
            print(f"🆕 Created and loaded table: {table}")

