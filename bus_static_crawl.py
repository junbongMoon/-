import os
import time
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

# --- zoneinfo 처리: 3.9+면 zoneinfo, 아니면 backports.zoneinfo ---

from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

from dotenv import load_dotenv
from typing import List, Dict

# =========================
# 환경변수 로드 (.env)
# =========================
load_dotenv()

# =========================
# 설정
# =========================
BASE_URL = "http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRouteAll"

SERVICE_KEY = os.getenv("BUS_API_KEY")
if not SERVICE_KEY:
    raise RuntimeError("환경변수 BUS_API_KEY가 없습니다. .env 또는 환경변수를 설정하세요.")

ROUTE_IDS = [
    "100100389",
    "113000004",
    "100100391",
    "100100392",
    "107000005",
    "100100400",
    "100100607",
    "107000006",
    "107000010",
    "111000020",
    "111000024",
    "113000005",
    "109000005",
]

DATA_DIR = Path("data")
PER_REQUEST_SLEEP_SEC = 1.2
TIMEOUT = (5, 15)
RETRIES = 2
KST = ZoneInfo("Asia/Seoul")


# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def daily_csv_path(ts: datetime) -> Path:
    """
    A 개선: 저장 파일을 '현재 시각(ts)' 기준 날짜로 결정
    """
    ensure_dir(DATA_DIR)
    return DATA_DIR / f"bus_data_{ts.strftime('%Y_%m_%d')}.csv"


def fetch_route_xml(route_id: str) -> str:
    params = {"serviceKey": SERVICE_KEY, "busRouteId": route_id}

    last_err = None
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()

            text = r.text.strip()
            if not text.startswith("<"):
                raise ValueError(f"Non-XML response (first 120 chars): {text[:120]}")
            return text

        except Exception as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))

    raise RuntimeError(f"Failed route_id={route_id}: {last_err}")


def parse_items_from_xml(xml_text: str) -> List[Dict]:
    """
    C 개선: headerCd 체크
    """
    root = ET.fromstring(xml_text)

    header_cd = (root.findtext(".//msgHeader/headerCd") or "").strip()
    header_msg = (root.findtext(".//msgHeader/headerMsg") or "").strip()
    if header_cd and header_cd != "0":
        raise RuntimeError(f"API error headerCd={header_cd}, msg={header_msg}")

    items = []
    for item in root.findall(".//itemList"):
        row = {}
        for child in item:
            row[child.tag] = (child.text or "").strip()
        items.append(row)
    return items


def get_existing_csv_columns(csv_path: Path) -> List[str]:
    """
    B 개선: 기존 CSV 헤더 읽기
    (파일이 없으면 빈 리스트)
    """
    if not csv_path.exists():
        return []
    try:
        # 헤더만 읽기
        return list(pd.read_csv(csv_path, nrows=0, encoding="utf-8-sig").columns)
    except Exception:
        # 혹시 인코딩/파일 깨짐 등 예외면 안전하게 빈 값 처리
        return []


def append_rows_to_daily_csv(rows: List[Dict], ts: datetime):
    """
    A: ts 기준 날짜 파일에 저장
    B: 컬럼 흔들림 방지 (기존 헤더 + 새 헤더 합집합으로 맞춤)
    """
    if not rows:
        return None

    csv_path = daily_csv_path(ts)

    df = pd.DataFrame(rows)
    df.insert(0, "fetched_at", ts.strftime("%Y-%m-%d %H:%M:%S"))

    # --- B: 컬럼 합집합으로 고정 ---
    existing_cols = get_existing_csv_columns(csv_path)
    new_cols = list(df.columns)

    # 기존 + 신규 (순서 유지)
    union_cols = existing_cols[:]
    for c in new_cols:
        if c not in union_cols:
            union_cols.append(c)

    # 누락된 컬럼 채우고 순서 맞추기
    for c in union_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[union_cols]

    write_header = not csv_path.exists()
    df.to_csv(csv_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")
    return csv_path


# =========================
# 스케줄 작업
# =========================
def job_collect_all_routes():
    job_started_at = now_kst()
    print(f"\n[{job_started_at.strftime('%Y-%m-%d %H:%M:%S %Z')}] Collect start")

    ok = 0
    fail = 0
    total_rows = 0
    last_out_path = None

    for route_id in ROUTE_IDS:
        # A 개선: route 처리 시각을 매번 다시 찍어서 날짜 넘어가면 파일 자동 분리
        ts = now_kst()

        try:
            xml = fetch_route_xml(route_id)
            rows = parse_items_from_xml(xml)

            # route_id는 응답에도 있지만, 누락 대비 보강
            for r in rows:
                r.setdefault("busRouteId", route_id)

            last_out_path = append_rows_to_daily_csv(rows, ts)
            ok += 1
            total_rows += len(rows)

            print(f"  ✅ {route_id} rows={len(rows)} appended -> {daily_csv_path(ts).name}")

        except Exception as e:
            fail += 1
            print(f"  ❌ {route_id} error -> {e}")

        time.sleep(PER_REQUEST_SLEEP_SEC)

    done_at = now_kst().strftime("%Y-%m-%d %H:%M:%S %Z")
    if last_out_path:
        print(f"[{done_at}] Collect done | ok={ok}, fail={fail}, rows={total_rows} (last file: {last_out_path})")
    else:
        print(f"[{done_at}] Collect done | ok={ok}, fail={fail}, rows={total_rows}")


def main():
    ensure_dir(DATA_DIR)

    scheduler = BlockingScheduler(timezone=KST)
    scheduler.add_job(
        job_collect_all_routes,
        trigger="cron",
        hour="5-23,0-2",
        minute="*/5",
        second=0
    )

    print("Scheduler started. Collecting every 5 minutes (05:00~02:55).")
    print(f"Saving CSV under: {DATA_DIR.resolve()}")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()