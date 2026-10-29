import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

def create_spark_session() -> SparkSession:
    """Phase 4와 자원을 분할 점유하도록 2코어로 제한한 SparkSession"""
    return SparkSession.builder \
        .appName("Bithumb-1Min-OHLCV-Aggregator") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. 빗썸 수신 틱 스키마 정의
    trade_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("buySellGb", StringType(), True),
        StructField("contPrice", StringType(), True),
        StructField("contQty", StringType(), True),
        StructField("contAmt", StringType(), True),
        StructField("contDtm", StringType(), True),
        StructField("updn", StringType(), True),
        StructField("ingest_timestamp", LongType(), True)
    ])

    # 2. Kafka 스트림 연결 (현재 유입 시점부터 실시간 모니터링)
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bithumb-trades") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 3. 역직렬화 및 Event Time 캐스팅
    trades_df = raw_kafka_df \
        .selectExpr("CAST(key AS STRING) as trade_symbol", "CAST(value AS STRING) as json_payload") \
        .select(
            col("trade_symbol"),
            from_json(col("json_payload"), trade_schema).alias("data")
        ) \
        .select(
            col("trade_symbol").alias("symbol"),
            col("data.contPrice").cast(DoubleType()).alias("price"),
            col("data.contQty").cast(DoubleType()).alias("quantity"),
            col("data.contAmt").cast(DoubleType()).alias("amount"),
            to_timestamp(col("data.contDtm"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_time")
        ) \
        .filter(col("event_time").isNotNull())

    # 4. 30초 늦은 데이터 허용 워터마크
    watermarked_trades = trades_df \
        .withWatermark("event_time", "30 seconds")

    # 5. 1분 텀블링 윈도우 OHLCV 집계
    ohlcv_df = watermarked_trades \
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute")
        ) \
        .agg(
            expr("min_by(price, event_time)").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            expr("max_by(price, event_time)").alias("close"),
            spark_sum("quantity").alias("volume"),
            spark_sum("amount").alias("total_amount"),
            spark_count("*").alias("trade_count")
        ) \
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("total_amount"),
            col("trade_count")
        )

    # 6. 실시간 콘솔 출력 (10초마다 갱신된 윈도우 행만 출력)
    query = ohlcv_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[INFO] Structured Streaming Aggregator started. Processing 1-min candles...")
    query.awaitTermination()

if __name__ == "__main__":
    main()