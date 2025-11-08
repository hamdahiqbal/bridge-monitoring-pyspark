import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date

# windows hadoop path
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

def main():
    spark = (
        SparkSession.builder
        .appName("BridgeBronzeIngest")
        .config("spark.hadoop.io.nativeio.useNativeIO", "false")
        .getOrCreate()
    )

    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("bridge_id", IntegerType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
    ])

    temp_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_temperature")
    )

    vib_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_vibration")
    )

    tilt_raw = (
        spark.readStream
        .schema(schema)
        .json("streams/bridge_tilt")
    )

    def enrich(df):
        return (
            df.withColumn("event_time_ts", to_timestamp("event_time"))
              .withColumn("ingest_time_ts", to_timestamp("ingest_time"))
              .withColumn("partition_date", to_date(col("event_time_ts")))
        )

    temp_df = enrich(temp_raw)
    vib_df  = enrich(vib_raw)
    tilt_df = enrich(tilt_raw)

    def split_valid_invalid(df):
        valid = df.where(col("event_time_ts").isNotNull() & col("value").isNotNull())
        invalid = df.where(col("event_time_ts").isNull() | col("value").isNull())
        return valid, invalid

    temp_valid, temp_invalid = split_valid_invalid(temp_df)
    vib_valid,  vib_invalid  = split_valid_invalid(vib_df)
    tilt_valid, tilt_invalid = split_valid_invalid(tilt_df)

    q_temp = (
        temp_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_temperature")
        .option("path", "bronze/bridge_temperature")
        .outputMode("append")
        .start()
    )

    q_vib = (
        vib_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_vibration")
        .option("path", "bronze/bridge_vibration")
        .outputMode("append")
        .start()
    )

    q_tilt = (
        tilt_valid.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_tilt")
        .option("path", "bronze/bridge_tilt")
        .outputMode("append")
        .start()
    )

    rejected_all = (
        temp_invalid.unionByName(vib_invalid, allowMissingColumns=True)
                    .unionByName(tilt_invalid, allowMissingColumns=True)
    )

    q_rejected = (
        rejected_all.writeStream
        .format("parquet")
        .option("checkpointLocation", "checkpoints/bronze_rejected")
        .option("path", "bronze/rejected")
        .outputMode("append")
        .start()
    )

    q_temp.awaitTermination()

if __name__ == "__main__":
    main()
