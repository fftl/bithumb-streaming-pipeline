from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "fftl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bithumb_daily_reconciliation",
    default_args=default_args,
    description="매일 새벽 01:00 KST 전일자 24시간 원천 틱 데이터 정산, 품질 검증 및 이메일 리포트",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 10, 28, tzinfo=local_tz),
    catchup=False,
    tags=["bithumb", "crypto", "batch", "reconciliation"],
) as dag:

    # Task 1: 전일자 원천 Parquet 파티션 존재 여부 검증
    check_raw_partition = BashOperator(
        task_id="check_raw_partition_exists",
        bash_command="""
        TARGET_DATE="{{ macros.ds_add(ds, -1) }}"
        PARTITION_DIR="/home/ubuntu/bithumb-streaming-pipeline/storage/raw_trades/date=${TARGET_DATE}"
        
        echo "[INFO] Checking existence of partition directory: ${PARTITION_DIR}"
        if [ ! -d "${PARTITION_DIR}" ]; then
            echo "[ERROR] Partition directory for ${TARGET_DATE} does not exist!"
            exit 1
        fi
        
        FILE_COUNT=$(find "${PARTITION_DIR}" -type f -name "*.parquet" | wc -l)
        if [ "${FILE_COUNT}" -eq 0 ]; then
            echo "[ERROR] No parquet files found in ${PARTITION_DIR}!"
            exit 1
        fi
        
        echo "[INFO] Validation passed: Found ${FILE_COUNT} parquet files for ${TARGET_DATE}."
        exit 0
        """,
    )

    # Task 2: 전일자 24시간 PySpark 정산 배치 실행 (요약 JSON 파일 생성)
    run_daily_reconcile = BashOperator(
        task_id="run_daily_reconcile_job",
        bash_command="""
        PROJECT_ROOT="/home/ubuntu/bithumb-streaming-pipeline"
        cd "${PROJECT_ROOT}"
        source .venv/bin/activate
        
        TARGET_DATE="{{ macros.ds_add(ds, -1) }}"
        echo "[INFO] Running daily batch reconciliation for target date: ${TARGET_DATE}"
        
        python processing/batch_daily_reconcile_2.py "${TARGET_DATE}"
        """,
    )

    # Task 3: 정산 결과 요약 JSON 기반 일일 브리핑 이메일 자동 발송
    send_reconcile_email = BashOperator(
        task_id="send_reconcile_email_job",
        bash_command="""
        PROJECT_ROOT="/home/ubuntu/bithumb-streaming-pipeline"
        cd "${PROJECT_ROOT}"
        source .venv/bin/activate
        
        TARGET_DATE="{{ macros.ds_add(ds, -1) }}"
        echo "[INFO] Sending reconciliation email report for: ${TARGET_DATE}"
        
        python monitoring/alert_email.py "${TARGET_DATE}"
        """,
    )

    # 태스크 순차 체이닝
    check_raw_partition >> run_daily_reconcile >> send_reconcile_email