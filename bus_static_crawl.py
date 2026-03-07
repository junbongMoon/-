import os
import time
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

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
CORE_ROUTES = [  # 항상 수집 대상(주말/공휴일 포함)
    "100100389",  # 9401
    "113000004",  # 9401-1
    "100100391",  # 9404
    "100100392",  # 9408
    "107000005",  # 9409
    "100100400",  # 9707
    "100100607",  # 9711
]

SPECIAL_MORNING_ONLY = [  # 평일 06:30~10:00만
    "107000006",  # 서울01출근
    "111000020",  # 서울03출근
    "113000005",  # 서울06출근
]

SPECIAL_EVENING_ONLY = [  # 평일 18:00~21:00만
    "107000010",  # 서울01퇴근
    "111000024",  # 서울03퇴근
    "109000005",  # 서울06퇴근
]

DATA_DIR = Path("data")
PER_REQUEST_SLEEP_SEC = 10.0
TIMEOUT = (5, 15)
RETRIES = 0
KST = ZoneInfo("Asia/Seoul")

# 호출 제한 감지 시 쿨다운
RATE_LIMIT_COOLDOWN_SEC = 600


# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def is_workday_kst(ts: datetime) -> bool:
    # 토/일 제외
    if ts.weekday() >= 5:
        return False

    # 공휴일 제외(패키지 있을 때만)
    if KR_HOLIDAYS is not None and ts.date() in KR_HOLIDAYS:
        return False

    return True


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def daily_csv_path(ts: datetime) -> Path:
    ensure_dir(DATA_DIR)
    return DATA_DIR / f"bus_data_{ts.strftime('%Y_%m_%d')}.csv"


def fetch_route_xml(route_id: str) -> str:
    params = {
        "serviceKey": SERVICE_KEY,
        "busRouteId": route_id
    }

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
    df.to_csv(
        csv_path,
        mode="a",
        index=False,
        header=write_header,
        encoding="utf-8-sig"
    )
    return csv_path


# =========================
# 수집 실행
# =========================
def collect_routes(route_ids: List[str], label: str, skip_on_holiday: bool = False):
    ts0 = now_kst()

    # SPECIAL 전용: 주말/공휴일이면 스킵
    if skip_on_holiday and not is_workday_kst(ts0):
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
            msg = str(e)
            print(f"  ❌ {route_id} error -> {e}")

            # 호출 제한 감지 시 이번 회차 중단
            if ("LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS" in msg) or ("headerCd=7" in msg):
                print(f"  🧊 Rate limit detected. Cooldown {RATE_LIMIT_COOLDOWN_SEC}s then stop this run.")
                time.sleep(RATE_LIMIT_COOLDOWN_SEC)
                break

        time.sleep(PER_REQUEST_SLEEP_SEC)

    done_at = now_kst().strftime("%Y-%m-%d %H:%M:%S %Z")
    if last_out_path:
        print(f"[{done_at}] Collect done | {label} | ok={ok}, fail={fail}, rows={total_rows} (last file: {last_out_path})")
    else:
        print(f"[{done_at}] Collect done | {label} | ok={ok}, fail={fail}, rows={total_rows}")


# =========================
# main
# =========================
def main():
    ensure_dir(DATA_DIR)

    scheduler = BlockingScheduler(
        timezone=KST,
        executors={"default": ThreadPoolExecutor(1)},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60
        }
    )

    # =====================================================
    # 1) CORE_ROUTES
    #    - 주말/공휴일 포함 계속 수집
    #    - 그래서 day_of_week 제거
    # =====================================================

    # 2분 수집: 아침
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min MORNING"),
        trigger="cron",
        hour="6-10",
        minute="*/2",
        second=0,
    )

    # 2분 수집: 저녁
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min EVENING (16:30~16:59)"),
        trigger="cron",
        hour="16",
        minute="30-59/2",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 2-min EVENING (17~20)"),
        trigger="cron",
        hour="17-20",
        minute="*/2",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE EVENING END (21:00)"),
        trigger="cron",
        hour="21",
        minute="0",
        second=0,
    )

    # 5분 수집: 나머지 시간대
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (00~02)"),
        trigger="cron",
        hour="0-2",
        minute="*/5",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (05)"),
        trigger="cron",
        hour="5",
        minute="*/5",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (11~15)"),
        trigger="cron",
        hour="11-15",
        minute="*/5",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (16:00~16:29)"),
        trigger="cron",
        hour="16",
        minute="0-29/5",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (21:05~21:59)"),
        trigger="cron",
        hour="21",
        minute="5-59/5",
        second=0,
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES, "CORE 5-min (22~23)"),
        trigger="cron",
        hour="22-23",
        minute="*/5",
        second=0,
    )

    # =====================================================
    # 2) SPECIAL_MORNING_ONLY
    #    - 스케줄은 걸어두되, 주말/공휴일이면 함수 내부에서 스킵
    # =====================================================
    scheduler.add_job(
        lambda: collect_routes(
            SPECIAL_MORNING_ONLY,
            "SPECIAL MORNING 2-min (06:30~06:59)",
            skip_on_holiday=True
        ),
        trigger="cron",
        hour="6",
        minute="30-59/2",
        second=30,
    )

    scheduler.add_job(
        lambda: collect_routes(
            SPECIAL_MORNING_ONLY,
            "SPECIAL MORNING 2-min (07~09)",
            skip_on_holiday=True
        ),
        trigger="cron",
        hour="7-9",
        minute="*/2",
        second=30,
    )

    scheduler.add_job(
        lambda: collect_routes(
            SPECIAL_MORNING_ONLY,
            "SPECIAL MORNING END (10:00)",
            skip_on_holiday=True
        ),
        trigger="cron",
        hour="10",
        minute="0",
        second=30,
    )

    # =====================================================
    # 3) SPECIAL_EVENING_ONLY
    #    - 스케줄은 걸어두되, 주말/공휴일이면 함수 내부에서 스킵
    # =====================================================
    scheduler.add_job(
        lambda: collect_routes(
            SPECIAL_EVENING_ONLY,
            "SPECIAL EVENING 2-min (18~20)",
            skip_on_holiday=True
        ),
        trigger="cron",
        hour="18-20",
        minute="*/2",
        second=30,
    )

    scheduler.add_job(
        lambda: collect_routes(
            SPECIAL_EVENING_ONLY,
            "SPECIAL EVENING END (21:00)",
            skip_on_holiday=True
        ),
        trigger="cron",
        hour="21",
        minute="0",
        second=30,
    )

    print("Scheduler started.")
    print(" - CORE: runs every day including weekends/holidays.")
    print(" - SPECIAL: skipped on weekends/holidays.")
    print(f"Saving CSV under: {DATA_DIR.resolve()}")
    print(f"PER_REQUEST_SLEEP_SEC={PER_REQUEST_SLEEP_SEC}, RATE_LIMIT_COOLDOWN_SEC={RATE_LIMIT_COOLDOWN_SEC}")

    print("\n[Registered Jobs]")
    for job in scheduler.get_jobs():
        print(f" - {job}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()