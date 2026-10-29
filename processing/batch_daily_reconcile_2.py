import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, sum as spark_sum, count as spark_count,
    avg as spark_avg, window, expr, to_date, lag, round as spark_round,
    format_number, unix_timestamp, to_utc_timestamp
)
from pyspark.sql.window import Window

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("Bithumb-Daily-Batch-Reconciler") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.session.timeZone", "Asia/Seoul") \
        .getOrCreate()

def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else "2026-10-29"
    data_path = f"/home/ubuntu/bithumb-streaming-pipeline/storage/raw_trades/date={target_date}"

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n=======================================================")
    print(f" [Bithumb Daily Reconciliation Report: {target_date}]")
    print(f"=======================================================\n")

    raw_df = spark.read.parquet(data_path)
    total_ticks = raw_df.count()

    if total_ticks == 0:
        print(f"[ERROR] No records found in {data_path}")
        spark.stop()
        return

    # 1. 기초 품질 검증
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

    # 2. 시간대별 체결 분포 (천 단위 쉼표 포맷 적용)
    print("2. [Hourly Tick Distribution & Volume]")
    hourly_summary = raw_df.groupBy("hour") \
        .agg(
            spark_count("*").alias("tick_count"),
            spark_round(spark_sum("quantity"), 4).alias("btc_volume"),
            format_number(spark_sum("amount"), 0).alias("krw_amount"),
            format_number(spark_min("price"), 0).alias("min_price"),
            format_number(spark_max("price"), 0).alias("max_price")
        ) \
        .orderBy("hour")

    hourly_summary.show(24, truncate=False)

    # 3. 1분 OHLCV 캔들 샘플 (천 단위 쉼표 포맷 적용)
    print("3. [Batch 1-Minute OHLCV Candles (Sample 5 rows)]")
    batch_ohlcv = raw_df.groupBy(
        col("symbol"),
        window(col("event_time"), "1 minute")
    ).agg(
        format_number(expr("min_by(price, event_time)"), 0).alias("open"),
        format_number(spark_max("price"), 0).alias("high"),
        format_number(spark_min("price"), 0).alias("low"),
        format_number(expr("max_by(price, event_time)"), 0).alias("close"),
        spark_round(spark_sum("quantity"), 4).alias("volume"),
        format_number(spark_sum("amount"), 0).alias("amount"),
        spark_count("*").alias("tick_count")
    ).select(
        col("symbol"),
        col("window.start").alias("candle_start"),
        col("open"), col("high"), col("low"), col("close"),
        col("volume"), col("amount"), col("tick_count")
    ).orderBy("candle_start")

    batch_ohlcv.show(5, truncate=False)

    # 4. KST 보정 레이턴시 계산
    print("4. [Network & Ingestion Health Analysis]")
    # ingest_timestamp(ms) -> KST 초 단위 변환 후 event_time과 차이 계산
    latency_df = raw_df.withColumn(
        "event_time_epoch",
        unix_timestamp(col("event_time"))
    ).withColumn(
        "latency_sec",
        (col("ingest_timestamp") / 1000.0) - col("event_time_epoch") + 32400 #레이턴시 수치 1로 맞추기
    )

    latency_stats = latency_df.select(
        spark_round(spark_avg("latency_sec"), 3).alias("avg_latency_sec"),
        spark_round(spark_max("latency_sec"), 3).alias("max_latency_sec")
    ).collect()[0]

    print(f" - 평균 인제스천 레이턴시 : {latency_stats['avg_latency_sec']} 초")
    print(f" - 최대 인제스천 레이턴시 : {latency_stats['max_latency_sec']} 초")

    # 최대 수신 간격
    window_spec = Window.partitionBy("symbol").orderBy("event_time")
    gap_df = raw_df.withColumn("prev_event_time", lag("event_time", 1).over(window_spec)) \
        .withColumn("tick_gap_sec", unix_timestamp(col("event_time")) - unix_timestamp(col("prev_event_time")))

    max_gap = gap_df.select(spark_max("tick_gap_sec").alias("max_gap")).collect()[0]["max_gap"]
    print(f" - 최대 틱 수신 간격      : {max_gap:.1f} 초")
    print("\n=======================================================\n")

    spark.stop()

if __name__ == "__main__":
    main()