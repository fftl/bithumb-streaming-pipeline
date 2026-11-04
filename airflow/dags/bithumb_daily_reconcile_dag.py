from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("Asia/Seoul")

# 정산 대상일 = 실행 시점(KST) 기준 전날
# data_interval_end는 이번 실행 시각(01:00 KST)이므로 KST로 바꾼 뒤 하루를 뺌
# (기존 ds_add(ds, -1)은 ds가 이미 과거 구간 시작일(UTC)이어서 사흘 전을 가리킬 수 있었음)
TARGET_DATE_TMPL = "{{ data_interval_end.in_timezone('Asia/Seoul').subtract(days=1).strftime('%Y-%m-%d') }}"
TASK_ENV = {"TARGET_DATE": TARGET_DATE_TMPL}

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
        env=TASK_ENV,
        append_env=True,
        bash_command="""
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
        env=TASK_ENV,
        append_env=True,
        bash_command="""
        PROJECT_ROOT="/home/ubuntu/bithumb-streaming-pipeline"
        cd "${PROJECT_ROOT}"
        source .venv/bin/activate
        
        echo "[INFO] Running daily batch reconciliation for target date: ${TARGET_DATE}"
        
        python processing/batch_daily_reconcile_2.py "${TARGET_DATE}"
        """,
    )

    # Task 3: 정산 결과 요약 JSON 기반 일일 브리핑 이메일 자동 발송
    send_reconcile_email = BashOperator(
        task_id="send_reconcile_email_job",
        env=TASK_ENV,
        append_env=True,
        trigger_rule="all_done",   # 앞 단계가 실패해도 실행 → 요약 JSON이 없으면 FAILED 메일 발송
        bash_command="""
        PROJECT_ROOT="/home/ubuntu/bithumb-streaming-pipeline"
        cd "${PROJECT_ROOT}"
        source .venv/bin/activate
        
        echo "[INFO] Sending reconciliation email report for: ${TARGET_DATE}"
        
        python monitoring/alert_email.py "${TARGET_DATE}"
        """,
    )

    # 태스크 순차 체이닝
    check_raw_partition >> run_daily_reconcile >> send_reconcile_email