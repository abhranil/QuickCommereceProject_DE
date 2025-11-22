#import asyncio
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lower, trim, when, initcap, lit
)
from pyspark.sql.types import (
    DecimalType, StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# === CONFIG ===
PROJECT_ID = "flawless-agency-474210-p4"  # 🔁 Replace with your project
DATASET = "quick_commerce_dataset"                      # 🔁 Replace with your dataset
TEMP_BUCKET = "dataproc-temp-asia-south1-925894589695-m1m9ef68"    # 🔁 Replace with your temp bucket (no gs:// prefix)

# Raw Data Buckets
ORDERS_PATH = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/OrdersData/"
INVENTORY_PATH = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/Inventorydata/"
STATUS_PATH = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/StatusEventData/"
GPS_PATH = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/GPSEventData/"

# === Spark Session ===
def get_spark_session():
    spark = (
        SparkSession.builder.appName("DataTransformationToBigQuery")
        #.config("spark.jars", "/usr/lib/spark/jars/spark-3.5-bigquery-0.42.4.jar")
        .config("spark.jars", "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/jars/spark-3.5-bigquery-0.42.4.jar")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .getOrCreate()
    )
    return spark

#spark.read.format("bigquery").option("table", "flawless-agency-474210-p4.quick_commerce_dataset.table").load().show()
# === Helper: Find latest CSV ===
def get_latest_file(spark, folder_path):
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.org.apache.hadoop.fs.Path(folder_path).toUri(), hadoop_conf
    )
    status = fs.globStatus(spark._jvm.org.apache.hadoop.fs.Path(folder_path + "/*.csv"))
    files = [str(file.getPath().toString()) for file in status if file.isFile()]
    if not files:
        raise FileNotFoundError(f"No CSV files found in {folder_path}")

    def extract_ts(filename):
        match = re.search(r"(\d{8}_\d{6})", filename)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            except:
                return datetime.min
        return datetime.min

    latest_file = max(files, key=extract_ts)
    print(f"📄 Latest file detected: {latest_file}")
    return latest_file


# === Orders Transformation ===
def transform_orders_data(spark): #async
    input_path = get_latest_file(spark, ORDERS_PATH)
    print("✅ Starting Orders Transformation Job...")

    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    print(f"📥 Orders Data Loaded: {orders_df.count()} records")

    orders_df = (
        orders_df.dropDuplicates(["order_id"])
        .filter(lower(trim(col("status"))) != "test")
        .withColumn("order_ts", col("order_ts").cast("timestamp"))
        .withColumn(
            "total_order_value",
            (col("quantity") * col("price").cast(DecimalType(10, 2)))
        )
        .withColumn(
            "status",
            when(lower(trim(col("status"))).isin("delivered", "complete"), "delivered")
            .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
            .when(lower(trim(col("status"))).isin("pending", "in progress", "processing"), "pending")
            .otherwise("unknown")
        )
    )

    orders_df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .option("table", f"{PROJECT_ID}:{DATASET}.orders_transformed") \
        .mode("overwrite") \
        .save()

    print(f"✅ Orders transformation complete and loaded to BigQuery table {DATASET}.orders_transformed")


# === Inventory Transformation ===
def transform_inventory_data(spark):#async
    input_path = get_latest_file(spark, INVENTORY_PATH)
    print("✅ Starting Inventory Transformation Job...")

    schema = StructType([
        StructField("item_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("stock_level", IntegerType(), True),
        StructField("last_update", StringType(), True),
    ])

    inventory_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 Inventory Data Loaded: {inventory_df.count()} records")

    inventory_df = (
        inventory_df
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("category", initcap(trim(col("category"))))
        .withColumn("stock_level", when(col("stock_level").isNull(), lit(0)).otherwise(col("stock_level")))
        .withColumn("stock_alert_flag", when(col("stock_level") == 0, lit(True)).otherwise(lit(False)))
        .withColumn("last_update", F.to_timestamp(col("last_update"), "dd-MM-yyyy HH:mm"))
    )

    inventory_df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .option("table", f"{PROJECT_ID}:{DATASET}.inventory_transformed") \
        .mode("overwrite") \
        .save()

    print(f"✅ Inventory transformation complete and loaded to BigQuery table {DATASET}.inventory_transformed")


# === Status Events Transformation ===
def transform_status_events_data(spark): #async
    input_path = get_latest_file(spark, STATUS_PATH)
    print("✅ Starting Status Events Transformation Job...")

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("courier_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
    ])

    status_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 Status Events Loaded: {status_df.count()} records")

    orders_bq = spark.read.format("bigquery").option("table", f"{PROJECT_ID}:{DATASET}.orders_transformed").load()
    print(f"📋 Orders Data Loaded from BQ: {orders_bq.count()} records")

    status_df = status_df.join(orders_bq.select("order_id"), on="order_id", how="inner")

    status_df = status_df.withColumn(
        "status",
        when(lower(trim(col("status"))).isin("picked_up", "pickup", "collected"), "picked_up")
        .when(lower(trim(col("status"))).isin("delivered", "completed"), "delivered")
        .when(lower(trim(col("status"))).isin("cancelled", "canceled"), "cancelled")
        .when(lower(trim(col("status"))).isin("in_transit", "on_the_way", "shipped"), "in_transit")
        .otherwise("unknown")
    )
    print(f"📥 Transforming Status Events Counts: {status_df.count()} records")

    status_df = status_df.withColumn("ts", F.to_timestamp(F.col("ts")))
    status_df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .option("table", f"{PROJECT_ID}:{DATASET}.status_events_transformed") \
        .mode("overwrite") \
        .save()

    print(f"✅ Status Events transformation complete and loaded to BigQuery table {DATASET}.status_events_transformed")


# === GPS Events Transformation ===
def transform_gps_events_data(spark): #async def
    input_path = get_latest_file(spark, GPS_PATH)
    print("✅ Starting GPS Events Transformation Job...")

    schema = StructType([
        StructField("courier_id", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("ts", StringType(), True),
    ])

    gps_df = spark.read.option("header", "true").schema(schema).csv(input_path)
    print(f"📥 GPS Events Loaded: {gps_df.count()} records")

    gps_df = gps_df.filter(
        (col("lat").between(-90, 90)) & (col("lon").between(-180, 180))
    ).withColumn("event_time", F.to_timestamp(F.col("ts"))).drop("ts")

    print(f"📥 Transforming GPS Events Counts: {gps_df.count()} records")

    courier_df = spark.read.format("bigquery").option("table", f"{PROJECT_ID}:{DATASET}.status_events_transformed").load()
    gps_df = gps_df.join(courier_df.select("courier_id", "status"), on="courier_id", how="left")

    gps_df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", TEMP_BUCKET) \
        .option("table", f"{PROJECT_ID}:{DATASET}.gps_events_transformed") \
        .mode("overwrite") \
        .save()

    print(f"✅ GPS Events transformation complete and loaded to BigQuery table {DATASET}.gps_events_transformed")


# === Main Entry Point ===
# async def main():
#     spark = get_spark_session()

#     await asyncio.gather(
#         transform_orders_data(spark),
#         transform_inventory_data(spark)
#     )
#     await asyncio.gather(
#         transform_status_events_data(spark),
#         transform_gps_events_data(spark)
#     )

#     spark.stop()
def main():
    spark = get_spark_session()

    transform_orders_data(spark)
    transform_inventory_data(spark)
    transform_status_events_data(spark)
    transform_gps_events_data(spark)

    spark.stop()
    print("🎉 All transformations completed and data loaded to BigQuery!")


if __name__ == "__main__":
   main()

