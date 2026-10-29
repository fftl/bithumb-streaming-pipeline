import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    avg as spark_avg, window, expr, to_date, lag, round as spark_round,
    from_unixtime
)
from pyspark.sql.window import Window

def create_spark_session() -> SparkSession:
    """배치 정산용 로컬 SparkSession (메모리 2GB, 코어 2개 할당)"""
    return SparkSession.builder \
        .appName("Bithumb-Daily-Batch-Reconciler") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-10-29"
    data_path = f"/home/ubuntu/bithumb-streaming-pipeline/storage/raw_trades/date={target_date}"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n=======================================================")
    print(f" [Bithumb Daily Reconciliation Report: {target_date}]")
    print(f"=======================================================\n")

    # 1. 대상 일자 Parquet 원천 데이터 로드
    raw_df = spark.read.parquet(data_path)
    total_ticks = raw_df.count()

    if total_ticks == 0:
        print(f"[ERROR] No records found in {data_path}")
        spark.stop()
        return

    # 2. 데이터 품질 및 기초 통계 검증 (Null 및 이상치 검사)
    quality_df = raw_df.select(
        spark_count(expr("CASE WHEN event_time IS NULL THEN 1 END")).alias("null_event_time"),
        spark_count(expr("CASE WHEN price <= 0 THEN 1 END")).alias("invalid_price"),
        spark_count(expr("CASE WHEN quantity <= 0 THEN 1 END")).alias("invalid_qty"),
        spark_min("event_time").alias("first_trade_dtm"),
        spark_max("event_time").alias("last_trade_dtm"),
        spark_sum("quantity").alias("total_btc_volume"),
        spark_sum("amount").alias("total_krw_amount")
    ).collect()[0]

    print("1. [Data Quality & Summary Metrics]")
    print(f" - 총 적재 틱(Tick) 수   : {total_ticks:,} 건")
    print(f" - 최초 체결 시각        : {quality_df['first_trade_dtm']}")
    print(f" - 마지막 체결 시각      : {quality_df['last_trade_dtm']}")
    print(f" - 총 체결 거래량        : {quality_df['total_btc_volume']:.4f} BTC")
    print(f" - 총 체결 거래대금      : {quality_df['total_krw_amount']:,.0f} KRW")
    print(f" - 결측/이상치 레코드 수 : {quality_df['null_event_time'] + quality_df['invalid_price'] + quality_df['invalid_qty']} 건\n")

    # 3. 시간대(Hour)별 체결 분포 분석
    print("2. [Hourly Tick Distribution & Volume]")
    hourly_summary = raw_df.groupBy("hour") \
        .agg(
            spark_count("*").alias("tick_count"),
            spark_round(spark_sum("quantity"), 4).alias("btc_volume"),
            spark_round(spark_sum("amount"), 0).alias("krw_amount"),
            spark_round(spark_min("price"), 0).alias("min_price"),
            spark_round(spark_max("price"), 0).alias("max_price")
        ) \
        .orderBy("hour")

    hourly_summary.show(24, truncate=False)

    # 4. 배치 1분 OHLCV 캔들 집계
    print("3. [Batch 1-Minute OHLCV Candles (Sample 5 rows)]")
    batch_ohlcv = raw_df.groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute")
    ).agg(
        expr("min_by(price, event_time)").alias("open"),
        spark_max("price").alias("high"),
        spark_min("price").alias("low"),
        expr("max_by(price, event_time)").alias("close"),
        spark_round(spark_sum("quantity"), 4).alias("volume"),
        spark_round(spark_sum("amount"), 0).alias("amount"),
        spark_count("*").alias("tick_count")
    ).select(
        col("symbol"),
        col("window.start").alias("candle_start"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
        col("amount"),
        col("tick_count")
    ).orderBy("candle_start")

    batch_ohlcv.show(5, truncate=False)

    # 5. 파이프라인 레이턴시 및 네트워크 갭(단절) 분석
    # 인입 지연시간(초): (ingest_timestamp / 1000) - unix_timestamp(event_time)
    print("4. [Network & Ingestion Health Analysis]")
    latency_df = raw_df.withColumn(
        "latency_sec",
        (col("ingest_timestamp") / 1000.0) - col("event_time").cast("double")
    )

    latency_stats = latency_df.select(
        spark_round(spark_avg("latency_sec"), 3).alias("avg_latency_sec"),
        spark_round(spark_max("latency_sec"), 3).alias("max_latency_sec")
    ).collect()[0]

    print(f" - 평균 인제스천 레이턴시 : {latency_stats['avg_latency_sec']} 초")
    print(f" - 최대 인제스천 레이턴시 : {latency_stats['max_latency_sec']} 초")

    # 틱 간 시간차(체결 간격) 최대치 계산 (네트워크 단절 여부 감지)
    window_spec = Window.partitionBy("symbol").orderBy("event_time")
    gap_df = raw_df.withColumn("prev_event_time", lag("event_time", 1).over(window_spec)) \
        .withColumn("tick_gap_sec", col("event_time").cast("double") - col("prev_event_time").cast("double"))

    max_gap = gap_df.select(spark_max("tick_gap_sec").alias("max_gap")).collect()[0]["max_gap"]
    print(f" - 최대 틱 수신 간격      : {max_gap:.1f} 초")
    print("\n=======================================================\n")

    spark.stop()

if __name__ == "__main__":
    main()