## Bithumb Streaming Pipeline

빗썸 BTC/KRW 실시간 체결 데이터를 수집해 Kafka로 전달하고, Spark Structured Streaming으로 원천 데이터와 1분봉(OHLCV)을 저장한 뒤, Airflow로 매일 전일자 데이터를 재계산해 품질을 검증하고 이메일로 보고하는 데이터 파이프라인입니다.