from airflow.models import Variable
from pyspark.sql import SparkSession
from datetime import datetime
from airflow.exceptions import AirflowFailException

def create_spark_session():
    return SparkSession.builder \
        .appName("PostgreSQL Export") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/Users/neelkalavadiya/Practicum_Project_Local/iceberg_warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def export_fact_table(spark, jdbc_url, jdbc_props):
    last_ingested_ts = Variable.get("last_fact_ingested_ts", default_var="2000-01-01T00:00:00")
    print(f"📌 Last fact export timestamp: {last_ingested_ts}")

    df = spark.read.table("local.gold.fact_charges") \
        .filter(f"gold_ingestion_ts > timestamp('{last_ingested_ts}')")

    row_count = df.count()
    if row_count == 0:
        print("⚠️ No new records to export for fact_charges.")
        return False  # Skip dim export

    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "test.fact_charges") \
            .options(**jdbc_props) \
            .mode("append") \
            .save()

        max_ts = df.agg({"gold_ingestion_ts": "max"}).collect()[0][0]
        Variable.set("last_fact_ingested_ts", max_ts.strftime("%Y-%m-%dT%H:%M:%S"))
        print(f"✅ Exported {row_count} rows to gold.fact_charges and updated Airflow Variable.")
        return True

    except Exception as e:
        print(f"❌ Failed to export fact_charges: {e}")
        raise AirflowFailException(str(e))


def export_dim_tables(spark, jdbc_url, jdbc_props):
    iceberg_tables = ["dim_hospital", "dim_payer", "dim_plan", "dim_procedure"]
    for table in iceberg_tables:
        print(f"📥 Exporting dimension table: local.gold.{table}")
        df = spark.read.table(f"local.gold.{table}")
        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", f"test.{table}") \
                .options(**jdbc_props) \
                .mode("overwrite") \
                .save()
            print(f"✅ Exported {table} to PostgreSQL")
        except Exception as e:
            print(f"❌ Failed to export {table} to PostgreSQL: {e}")
            raise


def export_gold_tables_to_postgres():
    spark = create_spark_session()

    jdbc_url = "jdbc:postgresql://localhost:5432/healthcare_insurance"
    jdbc_props = {
        "user": "postgres",
        "password": "201970",
        "driver": "org.postgresql.Driver"
    }

    export_fact_table(spark, jdbc_url, jdbc_props)
    export_dim_tables(spark, jdbc_url, jdbc_props)