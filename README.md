# 🚌 Seoul Bus Arrival Data Collector

서울시 버스 도착정보 API를 이용해 **버스 노선 도착 데이터를 5분마다 자동 수집하여 CSV 파일로 저장하는 Python 스케줄러 프로그램**입니다.

이 프로젝트는 서울 열린데이터 API(`getArrInfoByRouteAll`)를 호출하여 버스 도착 정보를 가져오고, 일자별 CSV 파일로 누적 저장합니다.

---

# 📌 프로젝트 개요

* **API**: 서울 열린데이터 버스 도착정보 API
* **수집 주기**: 5분마다
* **수집 시간**: 05:00 ~ 02:55
* **저장 형식**: CSV
* **저장 위치**: `data/` 폴더
* **파일명 형식**

```
bus_data_YYYY_MM_DD.csv
```

예시

```
data/bus_data_2026_03_05.csv
```

---

# 📂 프로젝트 구조

```
project/
│
├─ bus_scheduler.py
├─ requirements.txt
├─ .env
├─ README.md
│
└─ data/
   ├─ bus_data_2026_03_05.csv
   └─ bus_data_2026_03_06.csv
```

---

# ⚙️ 설치 방법

## 1️⃣ 저장소 클론

```
git clone https://github.com/your-repository-name.git
cd your-repository-name
```

---

## 2️⃣ 가상환경 생성 (권장)

```
python -m venv venv
```

Windows

```
venv\Scripts\activate
```

Mac / Linux

```
source venv/bin/activate
```

---

## 3️⃣ 패키지 설치

```
pip install -r requirements.txt
```

---

# 🔑 API KEY 설정

서울 열린데이터 포털에서 발급받은 **버스 API 키**를 `.env` 파일에 설정합니다.

`.env`

```
BUS_API_KEY=발급받은_API_KEY
```

예시

```
BUS_API_KEY=7N8KgrolAv7gC8C3+iYkvGUjTzg5h77VqFYHDPITgPJtMOtbju/cJX4TYdZbSqX/3WGF0KmaPwdWRFoHzX2qAA==
```

---

# ▶ 실행 방법

```
python bus_scheduler.py
```

실행 시 콘솔 출력 예시

```
Scheduler started. Collecting every 5 minutes (05:00~02:55).
Saving CSV under: C:\project\data

[2026-03-05 18:00:00 KST] Collect start
  ✅ 100100389 rows=42 appended -> bus_data_2026_03_05.csv
  ✅ 113000004 rows=36 appended -> bus_data_2026_03_05.csv
  ❌ 107000010 error -> API error headerCd=7
```

---

# ⏰ 스케줄 설정

현재 스케줄 설정

```
05:00 ~ 23:55
00:00 ~ 02:55
5분 간격 실행
```

코드

```python
scheduler.add_job(
    job_collect_all_routes,
    trigger="cron",
    hour="5-23,0-2",
    minute="*/5",
    second=0
)
```

---

# 💾 데이터 저장 방식

수집된 데이터는 **일자별 CSV 파일에 누적 저장**됩니다.

### 특징

* 날짜 기준 자동 파일 분리
* 기존 CSV 컬럼 유지
* 새로운 컬럼 자동 추가
* UTF-8 BOM 저장 (Excel 호환)

예시 컬럼

```
fetched_at
busRouteId
stNm
arrmsg1
arrmsg2
plainNo1
plainNo2
...
```

---

# 🔧 주요 기능

### 1️⃣ 자동 스케줄링

APScheduler를 이용하여 **5분마다 자동 실행**

### 2️⃣ API 에러 처리

* HTTP 오류 처리
* API headerCd 오류 처리
* 재시도 로직 포함

```
RETRIES = 2
```

### 3️⃣ CSV 컬럼 안정화

API 응답 컬럼이 변경되더라도 기존 CSV 구조를 유지하도록 **컬럼 합집합 방식** 적용

### 4️⃣ 날짜 기준 파일 분리

자정이 지나면 자동으로 **새 CSV 파일 생성**

---

# 📊 사용된 주요 라이브러리

* requests
* pandas
* APScheduler
* python-dotenv
* zoneinfo / backports.zoneinfo

---

# ⚠️ 주의사항

### API 호출 제한

서울 열린데이터 API는 **트래픽 제한**이 있을 수 있습니다.

요청 간 딜레이

```
PER_REQUEST_SLEEP_SEC = 0.2
```

---

### API 응답 오류

API 오류 발생 시 로그에 출력됩니다.

예시

```
API error headerCd=7
```

---

# 📈 활용 예시

수집된 데이터는 다음과 같은 프로젝트에 활용할 수 있습니다.

* 버스 도착시간 분석
* 버스 운행 패턴 분석
* 버스 혼잡도 예측
* 대중교통 데이터 분석
* 머신러닝 기반 도착시간 예측

---

# 📄 License

MIT License

---

# 👨‍💻 Author

Bus Data Collection Project
