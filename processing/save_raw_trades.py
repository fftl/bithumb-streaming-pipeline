import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

def create_spark_session() -> SparkSession:
    """OCI 인스턴스 자원을 고려한 경량 SparkSession 초기화"""
    return SparkSession.builder \
        .appName("Bithumb-Raw-Trades-Sink") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. 빗썸 수신 체결 JSON 스키마 선언
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

    # 2. Kafka 토픽 스트림 연결 (토픽의 첫 오프셋부터 모두 읽기)
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bithumb-trades") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 3. 역직렬화 및 파티셔닝용 날짜/시간 컬럼 추출
    trades_df = raw_kafka_df \
        .selectExpr("CAST(key AS STRING) as trade_symbol", "CAST(value AS STRING) as json_payload") \
        .select(
            col("trade_symbol"),
            from_json(col("json_payload"), trade_schema).alias("data")
        ) \
        .select(
            col("trade_symbol").alias("symbol"),
            col("data.buySellGb").alias("side"),
            col("data.contPrice").cast(DoubleType()).alias("price"),
            col("data.contQty").cast(DoubleType()).alias("quantity"),
            col("data.contAmt").cast(DoubleType()).alias("amount"),
            to_timestamp(col("data.contDtm"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_time"),
            col("data.updn"),
            col("data.ingest_timestamp")
        ) \
        .filter(col("event_time").isNotNull()) \
        .withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) \
        .withColumn("hour", date_format(col("event_time"), "HH"))

    # 4. Parquet Lakehouse 싱크 (60초 주기 Flush & 체크포인팅)
    query = trades_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "/home/ubuntu/bithumb-streaming-pipeline/storage/raw_trades") \
        .option("checkpointLocation", "/home/ubuntu/bithumb-streaming-pipeline/storage/checkpoints/raw_trades") \
        .partitionBy("date", "hour") \
        .trigger(processingTime="60 seconds") \
        .start()

    print("[INFO] Streaming Sink started. Writing raw trades to Parquet...")
    query.awaitTermination()

if __name__ == "__main__":
    main()