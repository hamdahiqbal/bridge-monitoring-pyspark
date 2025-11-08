from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)

def main():
    spark = (
        SparkSession.builder
        .appName("BridgeSilverEnrichment")
        .config("spark.hadoop.io.nativeio.useNativeIO", "false")
        .getOrCreate()
    )

    # ---- static bridges metadata schema ----
    bridges_schema = StructType([
        StructField("bridge_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("installation_date", StringType(), True),
    ])

    bridges_df = (
        spark.read
        .option("header", "true")
        .schema(bridges_schema)
        .csv("metadata/bridges.csv")
    )

    # ---- bronze schema (jo bronze_ingest ne likha hai) ----
    bronze_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("bridge_id", IntegerType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("event_time_ts", TimestampType(), True),
        StructField("ingest_time_ts", TimestampType(), True),
        StructField("partition_date", DateType(), True),
    ])

    temp_bronze = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load("bronze/bridge_temperature")
    )

    vib_bronze = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load("bronze/bridge_vibration")
    )

    tilt_bronze = (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load("bronze/bridge_tilt")
    )

    # ---- data quality rules ----
    temp_cond = (
        col("event_time_ts").isNotNull()
        & col("value").isNotNull()
        & (col("value") >= -40.0)
        & (col("value") <= 80.0)
    )

    vib_cond = (
        col("event_time_ts").isNotNull()
        & col("value").isNotNull()
        & (col("value") >= 0.0)
    )

    tilt_cond = (
        col("event_time_ts").isNotNull()
        & col("value").isNotNull()
        & (col("value") >= 0.0)
        & (col("value") <= 90.0)
    )

    temp_ok  = temp_bronze.where(temp_cond)
    vib_ok   = vib_bronze.where(vib_cond)
    tilt_ok  = tilt_bronze.where(tilt_cond)

    temp_bad = temp_bronze.where(~temp_cond)
    vib_bad  = vib_bronze.where(~vib_cond)
    tilt_bad = tilt_bronze.where(~tilt_cond)

    rejected_all = (
        temp_bad.unionByName(vib_bad, allowMissingColumns=True)
                .unionByName(tilt_bad, allowMissingColumns=True)
    )

    # ---- stream-static join ----
    temp_enriched = temp_ok.join(bridges_df, on="bridge_id", how="left")
    vib_enriched  = vib_ok.join(bridges_df, on="bridge_id", how="left")
    tilt_enriched = tilt_ok.join(bridges_df, on="bridge_id", how="left")

    # ---- writes ----
    q_temp = (
        temp_enriched.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/silver_temperature")
        .option("path", "silver/bridge_temperature")
        .outputMode("append")
        .start()
    )

    q_vib = (
        vib_enriched.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/silver_vibration")
        .option("path", "silver/bridge_vibration")
        .outputMode("append")
        .start()
    )

    q_tilt = (
        tilt_enriched.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/silver_tilt")
        .option("path", "silver/bridge_tilt")
        .outputMode("append")
        .start()
    )

    q_rejected = (
        rejected_all.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/silver_rejected")
        .option("path", "silver/rejected")
        .outputMode("append")
        .start()
    )

    q_temp.awaitTermination()

if __name__ == "__main__":
    main()
