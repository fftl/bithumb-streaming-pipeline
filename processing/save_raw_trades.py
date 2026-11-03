from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = f"{PROJECT_ROOT}/storage/raw_trades"
# 기존 체크포인트를 그대로 재사용해야 오프셋이 이어짐 (경로 변경 금지)
CHECKPOINT_PATH = f"{PROJECT_ROOT}/storage/checkpoints/raw_trades"


def create_spark_session() -> SparkSession:
    """OCI 인스턴스 자원을 고려한 경량 SparkSession 초기화"""
    return SparkSession.builder \
        .appName("Bithumb-Raw-Trades-Sink") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.session.timeZone", "Asia/Seoul") \
        .getOrCreate()
    # contDtm은 KST 벽시계 문자열이므로 세션 타임존을 KST로 고정해야
    # to_timestamp 결과가 실제 체결 시각(instant)과 일치함


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

    # 2. Kafka 토픽 스트림 연결 (체크포인트가 있으면 저장된 오프셋부터 재개)
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bithumb-trades") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 3. 역직렬화 + Kafka 메타데이터 보존 + 파티셔닝 컬럼 추출
    trades_df = raw_kafka_df \
        .select(
            col("key").cast("string").alias("trade_symbol"),
            from_json(col("value").cast("string"), trade_schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        ) \
        .select(
            col("trade_symbol").alias("symbol"),
            col("data.buySellGb").alias("side"),
            col("data.contPrice").cast(DoubleType()).alias("price"),
            col("data.contQty").cast(DoubleType()).alias("quantity"),
            col("data.contAmt").cast(DoubleType()).alias("amount"),
            to_timestamp(col("data.contDtm"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("event_time"),
            col("data.contDtm").alias("cont_dtm_raw"),          # 원문 보존 (재해석 가능)
            col("data.updn").alias("updn"),
            col("data.ingest_timestamp").alias("ingest_timestamp"),
            col("kafka_partition"),                               # 중복 검출 키 (partition, offset)
            col("kafka_offset"),                                  # 동률 정렬 키 + 신규 데이터 식별자
            col("kafka_timestamp"),                               # 브로커 적재 시각
        ) \
        .filter(col("event_time").isNotNull()) \
        .withColumn("date", date_format(col("event_time"), "yyyy-MM-dd")) \
        .withColumn("hour", date_format(col("event_time"), "HH"))

    # 4. Parquet Lakehouse 싱크 (60초 주기 Flush & 체크포인팅)
    query = trades_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("date", "hour") \
        .trigger(processingTime="60 seconds") \
        .start()

    print("[INFO] Streaming Sink started. Writing raw trades to Parquet...")
    query.awaitTermination()


if __name__ == "__main__":
    main()