import asyncio
import json
import logging
import time
from kafka import KafkaProducer
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("BithumbProducer")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "bithumb-trades"
BITHUMB_WS_URL = "wss://pubwss.bithumb.com/pub/ws"
SUBSCRIPTION_PAYLOAD = {
    "type": "transaction",
    "symbols": ["BTC_KRW"]
}

def create_kafka_producer() -> KafkaProducer:
    """처리량 극대화와 신뢰성을 고려한 KafkaProducer 인스턴스 초기화"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,                       # 리더 파티션 기록 확인 (처리량/안정성 균형)
        compression_type="gzip",      # 네트워크 대역폭 및 세그먼트 디스크 절감
        linger_ms=10,                 # 10ms 버퍼링을 통한 마이크로 배치 전송
        batch_size=32768,             # 32KB 배치 크기
        max_in_flight_requests_per_connection=5
    )

async def stream_bithumb_to_kafka():
    producer = create_kafka_producer()
    backoff_seconds = 1
    max_backoff = 60

    while True:
        try:
            logger.info("Connecting to Bithumb WebSocket: %s", BITHUMB_WS_URL)
            async with websockets.connect(
                BITHUMB_WS_URL,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                # 구독 메시지 전송
                await ws.send(json.dumps(SUBSCRIPTION_PAYLOAD))
                logger.info("Subscription sent: %s", SUBSCRIPTION_PAYLOAD)
                backoff_seconds = 1  # 연결 성공 시 백오프 초기화

                async for raw_message in ws:
                    data = json.loads(raw_message)

                    # 연결 확인 메시지 필터링
                    if data.get("status") == "0000":
                        logger.info("Connection handshake status: %s", data.get("resmsg"))
                        continue

                    # 체결 이벤트 언패킹 및 Kafka 전송
                    if data.get("type") == "transaction":
                        content = data.get("content", {})
                        trade_list = content.get("list", [])

                        for trade in trade_list:
                            symbol = trade.get("symbol")
                            # 인제스천 타임스탬프(Unix Epoch ms) 주입 (E2E 레이턴시 측정용)
                            trade["ingest_timestamp"] = int(time.time() * 1000)

                            # symbol을 Kafka Key로 지정하여 동일 코인은 동일 파티션 적재 (순서 보장)
                            producer.send(
                                topic=KAFKA_TOPIC,
                                key=symbol,
                                value=trade
                            )

        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
            logger.warning("WebSocket closed (%s). Reconnecting in %d seconds...", e, backoff_seconds)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff)

        except Exception as e:
            logger.error("Unexpected error occurred: %s", e, exc_info=True)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, max_backoff)

        finally:
            producer.flush()

if __name__ == "__main__":
    try:
        asyncio.run(stream_bithumb_to_kafka())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Exiting safely.")