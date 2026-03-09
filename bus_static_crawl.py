import os
import time
import threading
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
except ImportError:
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

from dotenv import load_dotenv
from typing import List, Dict, Optional

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

# =========================
# 노선 분류
# =========================
CORE_ROUTES_A = [
    "100100389",  # 9401
    "113000004",  # 9401-1
    "100100391",  # 9404
    "100100392",  # 9408
]

CORE_ROUTES_B = [
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
KST = ZoneInfo("Asia/Seoul")

TIMEOUT = (5, 15)
RETRIES = 1

# 개별 요청 사이 최소 간격(전역)
GLOBAL_MIN_REQUEST_INTERVAL_SEC = 12.0

# 한 회차(route loop) 시작 전 최소 간격
GLOBAL_MIN_RUN_GAP_SEC = 20.0

# 제한 오류 감지 시 전역 쿨다운
RATE_LIMIT_COOLDOWN_SEC = 600

# 네트워크 오류 시 재시도 전 대기
RETRY_BACKOFF_BASE_SEC = 1.0

# 프로그램 시작 직후 밀린 잡을 살리지 않도록 짧게
MISFIRE_GRACE_SEC = 5

# =========================
# 전역 상태
# =========================
_api_gate_lock = threading.Lock()
_last_request_monotonic = 0.0
_last_run_monotonic = 0.0
_block_api_until_monotonic = 0.0


# =========================
# 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def fmt_now() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S %Z")


def is_workday_kst(ts: datetime) -> bool:
    if ts.weekday() >= 5:
        return False
    if KR_HOLIDAYS is not None and ts.date() in KR_HOLIDAYS:
        return False
    return True


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def daily_csv_path(ts: datetime) -> Path:
    ensure_dir(DATA_DIR)
    return DATA_DIR / f"bus_data_{ts.strftime('%Y_%m_%d')}.csv"


def is_rate_limited_now() -> bool:
    global _block_api_until_monotonic
    return time.monotonic() < _block_api_until_monotonic


def set_global_cooldown(seconds: int, reason: str = "") -> None:
    global _block_api_until_monotonic
    until = time.monotonic() + seconds
    with _api_gate_lock:
        if until > _block_api_until_monotonic:
            _block_api_until_monotonic = until

    msg = f"[{fmt_now()}] 🧊 GLOBAL COOLDOWN set for {seconds}s"
    if reason:
        msg += f" | reason={reason}"
    print(msg)


def wait_for_api_slot(kind: str = "request") -> None:
    """
    전역 기준으로 request / run 간격을 강제.
    모든 job이 이 함수 하나를 거치도록 해서 연속 호출을 방지한다.
    """
    global _last_request_monotonic, _last_run_monotonic, _block_api_until_monotonic

    while True:
        sleep_sec = 0.0

        with _api_gate_lock:
            now_mono = time.monotonic()

            # 전역 쿨다운 중이면 대기
            if now_mono < _block_api_until_monotonic:
                sleep_sec = _block_api_until_monotonic - now_mono

            else:
                if kind == "run":
                    next_allowed = _last_run_monotonic + GLOBAL_MIN_RUN_GAP_SEC
                    if now_mono < next_allowed:
                        sleep_sec = next_allowed - now_mono
                    else:
                        _last_run_monotonic = now_mono
                        return

                else:  # request
                    next_allowed = _last_request_monotonic + GLOBAL_MIN_REQUEST_INTERVAL_SEC
                    if now_mono < next_allowed:
                        sleep_sec = next_allowed - now_mono
                    else:
                        _last_request_monotonic = now_mono
                        return

        if sleep_sec > 0:
            print(f"[{fmt_now()}] ⏳ wait {sleep_sec:.1f}s for global API slot ({kind})")
            time.sleep(sleep_sec)
        else:
            time.sleep(0.1)


def is_rate_limit_error_message(msg: str) -> bool:
    upper = (msg or "").upper()
    keywords = [
        "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS",
        "SERVICE REQUESTS EXCEEDS ERROR",
        "HEADERCD=7",
        "ERRORCODE=22",
        "인증코드 에러(22)",
    ]
    return any(k in upper for k in keywords)


def normalize_service_key(value: str) -> str:
    """
    .env에 이미 URL 인코딩된 값을 넣었든, 원문 키를 넣었든 requests가 알아서 처리하게 둔다.
    '%2B', '%2F', '%3D' 등이 들어있는 경우 한 번 decode 후 전달하는 것이 안전하다.
    """
    from urllib.parse import unquote
    return unquote(value.strip())


SERVICE_KEY = normalize_service_key(SERVICE_KEY)


# =========================
# API 호출/파싱
# =========================
def fetch_route_xml(route_id: str) -> str:
    params = {
        "serviceKey": SERVICE_KEY,
        "busRouteId": route_id
    }

    last_err: Optional[Exception] = None

    for attempt in range(RETRIES + 1):
        wait_for_api_slot("request")

        try:
            r = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
            r.raise_for_status()

            text = r.text.strip()
            if not text.startswith("<"):
                raise RuntimeError(f"Non-XML response: {text[:200]}")

            # 제한 메시지는 XML 안에 들어와도 여기서 한 번 빠르게 감지
            if "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS" in text.upper():
                raise RuntimeError("API error headerCd=7, msg=LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS")

            return text

        except Exception as e:
            last_err = e
            msg = str(e)

            if is_rate_limit_error_message(msg):
                set_global_cooldown(RATE_LIMIT_COOLDOWN_SEC, reason=msg)
                raise RuntimeError(msg)

            if attempt < RETRIES:
                backoff = RETRY_BACKOFF_BASE_SEC * (attempt + 1)
                print(f"[{fmt_now()}] retry route_id={route_id} after {backoff:.1f}s | err={msg}")
                time.sleep(backoff)

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


# =========================
# CSV 저장
# =========================
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

    if skip_on_holiday and not is_workday_kst(ts0):
        print(f"\n[{fmt_now()}] SKIP (weekend/holiday) | {label}")
        return

    # 쿨다운 중이면 회차 자체 skip
    if is_rate_limited_now():
        print(f"\n[{fmt_now()}] SKIP (global cooldown active) | {label}")
        return

    # run 시작도 전역 간격 제어
    wait_for_api_slot("run")

    print(f"\n[{fmt_now()}] Collect start | {label}")

    ok = 0
    fail = 0
    total_rows = 0
    last_out_path = None

    for route_id in route_ids:
        # 도중에 다른 job/요청에서 쿨다운 걸린 경우 즉시 중단
        if is_rate_limited_now():
            print(f"  🧊 stop current run because global cooldown is active | route_id={route_id}")
            break

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
            print(f"  ❌ {route_id} error -> {msg}")

            if is_rate_limit_error_message(msg):
                print(f"  🧊 Rate limit detected. Stop this run immediately.")
                break

    done_at = fmt_now()
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
            "misfire_grace_time": MISFIRE_GRACE_SEC
        }
    )

    core_all = CORE_ROUTES_A + CORE_ROUTES_B

    # =====================================================
    # 1) CORE_ROUTES
    #    - 주말/공휴일 포함 계속 수집
    #    - 2분 수집은 A/B 그룹으로 분할
    # =====================================================

    # 2분 수집: 아침
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_A, "CORE A 2-min MORNING"),
        trigger="cron",
        hour="6-10",
        minute="*/2",
        second=0,
        id="core_a_2min_morning"
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_B, "CORE B 2-min MORNING"),
        trigger="cron",
        hour="6-10",
        minute="1-59/2",
        second=0,
        id="core_b_2min_morning"
    )

    # 2분 수집: 저녁 16:30~16:59
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_A, "CORE A 2-min EVENING (16:30~16:58)"),
        trigger="cron",
        hour="16",
        minute="30-58/2",
        second=0,
        id="core_a_2min_evening_1630"
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_B, "CORE B 2-min EVENING (16:31~16:59)"),
        trigger="cron",
        hour="16",
        minute="31-59/2",
        second=0,
        id="core_b_2min_evening_1631"
    )

    # 2분 수집: 저녁 17~20
    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_A, "CORE A 2-min EVENING (17~20)"),
        trigger="cron",
        hour="17-20",
        minute="*/2",
        second=0,
        id="core_a_2min_evening_17_20"
    )

    scheduler.add_job(
        lambda: collect_routes(CORE_ROUTES_B, "CORE B 2-min EVENING (17~20)"),
        trigger="cron",
        hour="17-20",
        minute="1-59/2",
        second=0,
        id="core_b_2min_evening_17_20"
    )

    # 21:00 종료용
    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE ALL EVENING END (21:00)"),
        trigger="cron",
        hour="21",
        minute="0",
        second=0,
        id="core_all_evening_end"
    )

    # =====================================================
    # 2) CORE 5분 수집
    # =====================================================
    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (00~02)"),
        trigger="cron",
        hour="0-2",
        minute="*/5",
        second=0,
        id="core_5min_00_02"
    )

    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (05)"),
        trigger="cron",
        hour="5",
        minute="*/5",
        second=0,
        id="core_5min_05"
    )

    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (11~15)"),
        trigger="cron",
        hour="11-15",
        minute="*/5",
        second=0,
        id="core_5min_11_15"
    )

    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (16:00~16:29)"),
        trigger="cron",
        hour="16",
        minute="0-29/5",
        second=0,
        id="core_5min_16_00_29"
    )

    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (21:05~21:59)"),
        trigger="cron",
        hour="21",
        minute="5-59/5",
        second=0,
        id="core_5min_21_05_59"
    )

    scheduler.add_job(
        lambda: collect_routes(core_all, "CORE 5-min (22~23)"),
        trigger="cron",
        hour="22-23",
        minute="*/5",
        second=0,
        id="core_5min_22_23"
    )

    # =====================================================
    # 3) SPECIAL_MORNING_ONLY
    #    - 주말/공휴일 스킵
    #    - 30초 offset 유지
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
        id="special_morning_0630_0659"
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
        id="special_morning_07_09"
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
        id="special_morning_end"
    )

    # =====================================================
    # 4) SPECIAL_EVENING_ONLY
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
        id="special_evening_18_20"
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
        id="special_evening_end"
    )

    print("Scheduler started.")
    print(" - CORE: runs every day including weekends/holidays.")
    print(" - CORE 2-min: split into A/B groups.")
    print(" - CORE 5-min: all core routes.")
    print(" - SPECIAL: skipped on weekends/holidays.")
    print(f"Saving CSV under: {DATA_DIR.resolve()}")
    print(f"GLOBAL_MIN_REQUEST_INTERVAL_SEC={GLOBAL_MIN_REQUEST_INTERVAL_SEC}")
    print(f"GLOBAL_MIN_RUN_GAP_SEC={GLOBAL_MIN_RUN_GAP_SEC}")
    print(f"RATE_LIMIT_COOLDOWN_SEC={RATE_LIMIT_COOLDOWN_SEC}")
    print(f"MISFIRE_GRACE_SEC={MISFIRE_GRACE_SEC}")

    print("\n[Registered Jobs]")
    for job in scheduler.get_jobs():
        try:
            print(f" - {job.id} | next_run_time={job.next_run_time}")
        except Exception:
            print(f" - {job}")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()