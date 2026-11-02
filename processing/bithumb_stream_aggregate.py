from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, struct, current_timestamp, date_format,
    min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = f"{PROJECT_ROOT}/storage/ohlcv_1m"
CHECKPOINT_PATH = f"{PROJECT_ROOT}/storage/checkpoints/ohlcv_1m"

def create_spark_session() -> SparkSession:
    return (SparkSession.builder
        .appName("Bithumb-1Min-OHLCV-Aggregator")
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "Asia/Seoul")   # 3. 타임존 명시
        .getOrCreate())

trade_schema = StructType([
    StructField("symbol", StringType()), StructField("contPrice", StringType()),
    StructField("contQty", StringType()), StructField("contAmt", StringType()),
    StructField("contDtm", StringType()), StructField("ingest_timestamp", LongType()),
])

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "bithumb-trades")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    trades = (raw
        .select(
            col("key").cast("string").alias("symbol"),
            from_json(col("value").cast("string"), trade_schema).alias("d"),
            col("offset").alias("kafka_offset"),                 # 4. 동률 보조 키
        )
        .select(
            "symbol", "kafka_offset",
            col("d.contPrice").cast(DoubleType()).alias("price"),
            col("d.contQty").cast(DoubleType()).alias("quantity"),
            col("d.contAmt").cast(DoubleType()).alias("amount"),
            to_timestamp(col("d.contDtm"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_time"),
        )
        .filter(col("event_time").isNotNull()))

    ohlcv = (trades
        .withWatermark("event_time", "30 seconds")
        .groupBy(col("symbol"), window(col("event_time"), "1 minute"))
        .agg(
            spark_min(struct("event_time", "kafka_offset", "price")).alias("_first"),
            spark_max(struct("event_time", "kafka_offset", "price")).alias("_last"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            spark_sum("quantity").alias("volume"),
            spark_sum("amount").alias("total_amount"),
            spark_count("*").alias("trade_count"),
        )
        .select(
            "symbol",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("_first.price").alias("open"), "high", "low",
            col("_last.price").alias("close"),
            "volume", "total_amount", "trade_count",
            col("_first.event_time").alias("first_event_time"),   # 6. 감사 컬럼
            col("_last.event_time").alias("last_event_time"),
            current_timestamp().alias("processed_at"),
            date_format(col("window.start"), "yyyy-MM-dd").alias("date"),
        ))

    query = (ohlcv.writeStream
        .format("parquet")
        .outputMode("append")                                      # 1. 확정 캔들만 기록
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)             # 2. 체크포인트
        .partitionBy("date")
        .trigger(processingTime="1 minute")                        # 5. 소형 파일 완화
        .start())

    query.awaitTermination()

if __name__ == "__main__":
    main()