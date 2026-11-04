import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# .env 파일 수동 로드 (추가 라이브러리 의존성 배제)
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
if ENV_PATH.exists():
    with open(ENV_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key, _, value = line.strip().partition("=")
                os.environ[key] = value.strip('"').strip("'")

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
ALERT_RECEIVER = os.getenv("ALERT_RECEIVER")


def send_reconciliation_email(
    target_date: str, status: str = "SUCCESS", details: dict = None
):
    if not SMTP_USER or not SMTP_PASSWORD or not ALERT_RECEIVER:
        print(
            "[ERROR] SMTP 환경변수(SMTP_USER, SMTP_PASSWORD, ALERT_RECEIVER)가 설정되지 않았습니다."
        )
        return False

    if details is None:
        details = {}

    status_color = "#2ECC71" if status == "SUCCESS" else "#E74C3C"
    subject = f"[정산 리포트] {target_date} 빗썸 BTC 원천 틱 데이터 검증 결과 ({status})"

    # HTML 메일 본문 템플릿
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background-color: #f8f9fa; padding: 20px; }}
            .container {{ max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 8px; border: 1px solid #e9ecef; overflow: hidden; }}
            .header {{ background-color: #1a1a2e; color: #ffffff; padding: 20px; }}
            .header h2 {{ margin: 0; font-size: 18px; }}
            .status-badge {{ display: inline-block; padding: 4px 10px; border-radius: 4px; font-weight: bold; background-color: {status_color}; color: #ffffff; font-size: 12px; margin-top: 8px; }}
            .content {{ padding: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 14px; }}
            th, td {{ padding: 12px 10px; border-bottom: 1px solid #edf2f7; text-align: left; }}
            th {{ background-color: #f8f9fa; color: #4a5568; font-weight: 600; width: 40%; }}
            td {{ color: #2d3748; }}
            .footer {{ background-color: #f8f9fa; padding: 15px 20px; font-size: 12px; color: #718096; border-top: 1px solid #e9ecef; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>빗썸 BTC 일별 정산 및 무결성 검증 리포트</h2>
                <div class="status-badge">{status}</div>
            </div>
            <div class="content">
                <p style="margin: 0 0 15px 0; color: #4a5568; font-size: 14px;">
                    <strong>{target_date}</strong> 일자의 24시간 원천 체결 틱 데이터 배치 정산 및 품질 진단이 완료되었습니다.
                </p>
                <table>
                    <tr>
                        <th>정산 대상 일자</th>
                        <td>{target_date}</td>
                    </tr>
                    <tr>
                        <th>총 수신 틱 수</th>
                        <td><strong>{details.get('total_ticks', 'N/A')}</strong> 건</td>
                    </tr>
                    <tr>
                        <th>총 거래대금 (KRW)</th>
                        <td>{details.get('total_amount', 'N/A')} 원</td>
                    </tr>
                    <tr>
                        <th>1분봉 생성 수 (결측 검증)</th>
                        <td>{details.get('generated_candles', 'N/A')} / 1,440 개 (결측: {details.get('missing_candles', 0)} 건)</td>
                    </tr>
                    <tr>
                        <th>평균 인제스천 레이턴시</th>
                        <td>{details.get('avg_latency', 'N/A')} 초</td>
                    </tr>
                    <tr>
                        <th>최대 인제스천 레이턴시</th>
                        <td>{details.get('max_latency', 'N/A')} 초</td>
                    </tr>
                    <tr>
                        <th>최대 틱 수신 간격</th>
                        <td>{details.get('max_gap', 'N/A')} 초</td>
                    </tr>
                </table>
            </div>
            <div class="footer">
                OCI ARM64 Pipeline Engine &bull; Spark 3.5.1 &bull; Airflow 2.9.3
            </div>
        </div>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Bithumb Data Pipeline <{SMTP_USER}>"
    msg["To"] = ALERT_RECEIVER
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, [ALERT_RECEIVER], msg.as_string())
        print(f"[INFO] Daily reconciliation email successfully sent to {ALERT_RECEIVER}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send email: {e}", file=sys.stderr)
        return False


if __name__ == "__main__":
    import json
    from pathlib import Path

    target_date = (
        sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    )
    base_dir = Path(__file__).resolve().parent.parent
    summary_file = base_dir / f"storage/reconcile_reports/summary_{target_date}.json"

    if summary_file.exists():
        with open(summary_file, "r", encoding="utf-8") as f:
            report_data = json.load(f)

        sent = send_reconciliation_email(
            target_date=report_data.get("target_date", target_date),
            status=report_data.get("status", "SUCCESS"),
            details=report_data,
        )
    else:
        print(
            f"[WARN] Summary file not found: {summary_file}. Sending failure alert."
        )
        sent = send_reconciliation_email(
            target_date=target_date,
            status="FAILED",
            details={
                "total_ticks": "0",
                "total_amount": "0",
                "generated_candles": "0",
                "missing_candles": 1440,
                "avg_latency": "N/A",
                "max_latency": "N/A",
                "max_gap": "N/A",
            },
        )

    # 발송 실패 시 Airflow Task도 실패로 표시되도록 종료 코드 반환
    sys.exit(0 if sent else 1)