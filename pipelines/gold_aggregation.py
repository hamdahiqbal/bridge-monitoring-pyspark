from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    avg,
    max as spark_max,
)
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
        .appName("BridgeGoldAggregation")
        .config("spark.hadoop.io.nativeio.useNativeIO", "false")
        .getOrCreate()
    )

    # ----- silver schema infer from static read -----
    sample_silver = spark.read.parquet("silver/bridge_temperature")
    silver_schema = sample_silver.schema

    temp_silver = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .load("silver/bridge_temperature")
    )

    vib_silver = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .load("silver/bridge_vibration")
    )

    tilt_silver = (
        spark.readStream
        .format("parquet")
        .schema(silver_schema)
        .load("silver/bridge_tilt")
    )

    # ----- 2-min watermark + 1-min tumbling window -----
    temp_agg = (
        temp_silver
        .withWatermark("event_time_ts", "2 minutes")
        .groupBy(
            col("bridge_id"),
            window(col("event_time_ts"), "1 minute"),
        )
        .agg(
            avg("value").alias("avg_temperature")
        )
    )

    vib_agg = (
        vib_silver
        .withWatermark("event_time_ts", "2 minutes")
        .groupBy(
            col("bridge_id"),
            window(col("event_time_ts"), "1 minute"),
        )
        .agg(
            spark_max("value").alias("max_vibration")
        )
    )

    tilt_agg = (
        tilt_silver
        .withWatermark("event_time_ts", "2 minutes")
        .groupBy(
            col("bridge_id"),
            window(col("event_time_ts"), "1 minute"),
        )
        .agg(
            spark_max("value").alias("max_tilt_angle")
        )
    )

    # ----- stream-stream joins on (bridge_id, window) -----
    temp_vib = temp_agg.join(
        vib_agg,
        on=["bridge_id", "window"],
        how="inner",
    )

    metrics = temp_vib.join(
        tilt_agg,
        on=["bridge_id", "window"],
        how="inner",
    )

    # ----- flatten window & select final columns -----
    result = metrics.select(
        col("bridge_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("max_vibration"),
        col("max_tilt_angle"),
    )

    query = (
        result.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/gold_bridge_metrics")
        .option("path", "gold/bridge_metrics")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
