import os
import time
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

# --- zoneinfo 처리: 3.9+면 zoneinfo, 아니면 backports.zoneinfo ---
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # Python 3.8 이하
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

from dotenv import load_dotenv
from typing import List, Dict

# =========================
# (옵션) 공휴일 체크
# =========================
try:
    import holidays  # pip install holidays
    KR_HOLIDAYS = holidays.KR()
except Exception:
    KR_HOLIDAYS = None

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

# --- 노선 분류 ---
CORE_ROUTES = [  # 항상 수집 대상(단, 시간별 주기 다름)
    "100100389",  # 9401
    "113000004",  # 9401-1
    "100100391",  # 9404
    "100100392",  # 9408
    "107000005",  # 9409
    "100100400",  # 9707
    "100100607",  # 9711
]

SPECIAL_MORNING_ONLY = [  # 06:30~10:00만
    "107000006",  # 서울01출근
    "111000020",  # 서울03출근
    "113000005",  # 서울06출근
]

SPECIAL_EVENING_ONLY = [  # 18:00~21:00만
    "107000010",  # 서울01퇴근
    "111000024",  # 서울03퇴근
    "109000005",  # 서울06퇴근
]

DATA_DIR = Path("data")
PER_REQUEST_SLEEP_SEC = 1.5  
TIMEOUT = (5, 15)
RETRIES = 2
KST = ZoneInfo("Asia/Seoul")


# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def is_workday_kst(ts: datetime) -> bool:
    # 토/일 제외
    if ts.weekday() >= 5:
        return False
    # 공휴일 제외(패키지 없으면 패스)
    if KR_HOLIDAYS is not None:
        if ts.date() in KR_HOLIDAYS:
            return False
    return True


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def daily_csv_path(ts: datetime) -> Path:
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
    if not csv_path.exists():
        return []
    try:
        return list(pd.read_csv(csv_path, nrows=0, encoding="utf-8-sig").columns)
    except Exception:
        return []


def append_rows_to_daily_csv(rows: List[Dict], ts: datetime):
    if not rows:
        return None

    csv_path = daily_csv_path(ts)

    df = pd.DataFrame(rows)
    df.insert(0, "fetched_at", ts.strftime("%Y-%m-%d %H:%M:%S"))

    existing_cols = get_existing_csv_columns(csv_path)
    new_cols = list(df.columns)

    union_cols = existing_cols[:]
    for c in new_cols:
        if c not in union_cols:
            union_cols.append(c)

    for c in union_cols:
        if c not in df.columns:
            df[c] = ""

    df = df[union_cols]

    write_header = not csv_path.exists()
    df.to_csv(csv_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")
    return csv_path


# =========================
# 수집 실행(노선 리스트 인자로)
# =========================
def collect_routes(route_ids: List[str], label: str):
    ts0 = now_kst()

    # ✅ 토/일/공휴일 스킵
    if not is_workday_kst(ts0):
        print(f"\n[{ts0.strftime('%Y-%m-%d %H:%M:%S %Z')}] SKIP (weekend/holiday) | {label}")
        return

    print(f"\n[{ts0.strftime('%Y-%m-%d %H:%M:%S %Z')}] Collect start | {label}")

    ok = 0
    fail = 0
    total_rows = 0
    last_out_path = None

    for route_id in route_ids:
        ts = now_kst()
        try:
            xml = fetch_route_xml(route_id)
            rows = parse_items_from_xml(xml)

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
        print(f"[{done_at}] Collect done | {label} | ok={ok}, fail={fail}, rows={total_rows} (last file: {last_out_path})")
    else:
        print(f"[{done_at}] Collect done | {label} | ok={ok}, fail={fail}, rows={total_rows}")


# =========================
# main: 스케줄 분리 등록
# =========================
def main():
    ensure_dir(DATA_DIR)

    scheduler = BlockingScheduler(
        timezone=KST,
        job_defaults={
            "coalesce": True,      # 밀린 작업은 합쳐서 1번만
            "max_instances": 1,    # 동시에 중복 실행 방지
            "misfire_grace_time": 60
        }
    )

    # -------------------------
    # 1) CORE_ROUTES: 출퇴근 2분
    # - 출근: 06:00~10:59 (*/2)
    # - 퇴근: 16:30~16:59 (30-59/2), 17:00~20:59 (*/2), 21:00 (0)
    # -------------------------
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min MORNING"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="6-10",
        minute="*/2",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min EVENING (16:30~16:59)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="16",
        minute="30-59/2",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min EVENING (17~20)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="17-20",
        minute="*/2",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE EVENING END (21:00)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="21",
        minute="0",
        second=0,
    )

    # -------------------------
    # 2) CORE_ROUTES: 그 외 5분
    # - 00:00~02:59  (*/5)
    # - 05:00~05:59  (*/5)
    # - 11:00~15:59  (*/5)
    # - 16:00~16:29  (0-29/5)
    # - 21:05~21:59  (5-59/5)
    # - 22:00~23:59  (*/5)
    # -------------------------
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (00~02)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="0-2",
        minute="*/5",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (05)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="5",
        minute="*/5",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (11~15)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="11-15",
        minute="*/5",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (16:00~16:29)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="16",
        minute="0-29/5",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (21:05~21:59)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="21",
        minute="5-59/5",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (22~23)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="22-23",
        minute="*/5",
        second=0,
    )

    # -------------------------
    # 3) SPECIAL 출근: 06:30~10:00만 (2분)
    # -------------------------
    scheduler.add_job(
        lambda: collect_routes(SPECIAL_MORNING_ONLY, "SPECIAL MORNING 2-min (06:30~06:59)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="6",
        minute="30-59/2",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(SPECIAL_MORNING_ONLY, "SPECIAL MORNING 2-min (07~09)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="7-9",
        minute="*/2",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(SPECIAL_MORNING_ONLY, "SPECIAL MORNING END (10:00)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="10",
        minute="0",
        second=0,
    )

    # -------------------------
    # 4) SPECIAL 퇴근: 18:00~21:00만 (2분)
    # -------------------------
    scheduler.add_job(
        lambda: collect_routes(SPECIAL_EVENING_ONLY, "SPECIAL EVENING 2-min (18~20)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="18-20",
        minute="*/2",
        second=0,
    )
    scheduler.add_job(
        lambda: collect_routes(SPECIAL_EVENING_ONLY, "SPECIAL EVENING END (21:00)"),
        trigger="cron",
        day_of_week="mon-fri",
        hour="21",
        minute="0",
        second=0,
    )

    print("Scheduler started.")
    print(" - Weekdays only (Mon~Fri), no weekends, no holidays (if holidays pkg installed).")
    print(f"Saving CSV under: {DATA_DIR.resolve()}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()