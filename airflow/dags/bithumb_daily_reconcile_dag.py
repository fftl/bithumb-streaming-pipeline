from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fftl",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PIPELINE_ROOT = "/home/ubuntu/bithumb-streaming-pipeline"
VENV_PYTHON = f"{PIPELINE_ROOT}/.venv/bin/python"
SCRIPT_PATH = f"{PIPELINE_ROOT}/processing/batch_daily_reconcile_2.py"
STORAGE_ROOT = f"{PIPELINE_ROOT}/storage/raw_trades"

with DAG(
    dag_id="bithumb_daily_reconciliation",
    default_args=default_args,
    description="매일 새벽 전일자 빗썸 비트코인 원천 틱 데이터 품질 검증 및 배치 정산",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 10, 28),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "bithumb", "spark", "batch"],
) as dag:

    # 1. 대상 날짜 Parquet 파티션 디렉토리 검사
    # Jinja 템플릿({{ ds }})을 env로 넘겨 f-string 충돌을 방지
    check_raw_partition = BashOperator(
        task_id="check_raw_partition_exists",
        env={"TARGET_DATE": "{{ ds }}"},
        bash_command=f"""
        TARGET_DIR="{STORAGE_ROOT}/date=$TARGET_DATE"
        
        echo "[INFO] Checking raw partition directory: $TARGET_DIR"
        if [ ! -d "$TARGET_DIR" ]; then
            echo "[ERROR] Raw trades partition does not exist: $TARGET_DIR"
            exit 1
        fi
        
        FILE_COUNT=$(ls -1 "$TARGET_DIR"/*/*.parquet 2>/dev/null | wc -l)
        echo "[INFO] Found $FILE_COUNT parquet files in $TARGET_DIR"
        
        if [ "$FILE_COUNT" -eq 0 ]; then
            echo "[ERROR] No parquet files found in target partition!"
            exit 1
        fi
        """,
    )

    # 2. PySpark 배치 정산 스크립트 실행
    run_daily_reconcile = BashOperator(
        task_id="run_daily_reconcile_job",
        env={"TARGET_DATE": "{{ ds }}"},
        bash_command=f"{VENV_PYTHON} {SCRIPT_PATH} $TARGET_DATE",
    )

    check_raw_partition >> run_daily_reconcile