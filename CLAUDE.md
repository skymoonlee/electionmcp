# Korea Election MCP — 9회 전국동시지방선거 (2026.6.3)

## 프로젝트 목적
중앙선관위 OpenAPI + 후보자정보공개자료(PDF) 를 수집해 HuggingFace Dataset 으로
배포하고, MCP 서버를 통해 유권자가 자기 지역구 후보 정보를 조회·비교할 수 있게 한다.

법적 근거: 공직선거법 제49조 (후보자정보공개제도) — 국민의 알권리·선거권 행사 보장 목적.

## 핵심 규칙
1. **MCP 응답에 후보 추천/평가/우열 멘트 금지.** 사실 조회·비교만.
2. 모든 응답에 출처(중앙선거관리위원회) 명시.
3. 선거 6일 전부터 여론조사·지지율 관련 정보 제공 금지 (공직선거법 §108).
4. 후보자정보공개자료(전과/재산/병역/학력/납세) PDF는 텍스트 추출 후 구조화해서 저장,
   raw PDF 그대로 HF에 올리지 않음.

## NEC OpenAPI 엔드포인트 (활용신청 완료)

베이스 URL: `https://apis.data.go.kr/9760000`  ← **반드시 https**

### 1. CommonCodeService (코드 정보)
- `/getCommonSgCodeList` — 선거ID·선거종류·선거명 (sgId 확정용)
- `/getCommonGusigunCodeList` — 구시군 코드
- `/getCommonSggCodeList` — 선거구 코드 + **선출정수**
- `/getCommonPartyCodeList` — 정당 코드
- `/getCommonJobCodeList` — 직업 코드
- `/getCommonEduBckgrdCodeList` — 학력 코드

### 2. ElecPrmsInfoInqireService (선거공약 정보)
- `/getCnddtElecPrmsInfoInqire` — 후보자 ID로 공약 조회
- 공약 제출 의무: sg_type ∈ {3 시·도지사, 4 구·시·군의장, 11 교육감}만

### 3. PofelcddInfoInqireService (후보자 정보)
- `/getPoelpcddRegistSttusInfoInqire` — **예비후보자** (~5/14, 등록일부터 무효)
- `/getPofelcddRegistSttusInfoInqire` — **정식후보자** (5/15 등록일 이후 ~ 선거일)
- ⚠️ 두 엔드포인트 이름이 비슷해서 혼동 주의: `Poelpcdd` vs `Pofelcdd`

## 선거종류코드 (sgTypecode)
- `3` 시·도지사
- `4` 구·시·군의장
- `5` 시·도의원(지역구)
- `6` 구·시·군의원(지역구)
- `7` 시·도의원(비례)
- `8` 구·시·군의원(비례)
- `11` 교육감

## 타임라인 (2026 9회 지선) — NEC 검색 검증

| 일자 | 이벤트 | API 영향 |
|---|---|---|
| 2/20 ~ | 시·도의원·시의원·장 예비후보자 등록 시작 | preliminary 조회 가능 |
| 3/22 ~ | 군의원·군수 예비후보자 등록 시작 | preliminary 조회 가능 |
| **5/14 09:00 ~ 5/15 18:00** | **정식 후보자 등록 신청 (2일간)** | 이 순간부터 예비후보자 데이터 사라짐 |
| 5/16 ~ | 정식후보자 + 정보공개자료 활성화 | official 조회 + PDF 수집 |
| 5/21 | 선거기간 개시 | |
| 5/28 ~ | 여론조사 공표 금지 (공직선거법 §108) | (해당 없음 — 정보 제공만) |
| 5/29 ~ 30 | 사전투표 | |
| **6/3** | 선거일 | NEC 정보공개 PDF 비공개 전환 |
| 6/4 ~ | 당선인 정보 API 활용 (별도 신청 필요) | |

권장 풀스캔 타이밍: **5/15 23:00** (정식등록 마감 직후) + **5/16 02:00** PDF + OCR.

## 4단계 파이프라인 (Stage)
1. **stage 1** — 후보자 인덱스 수집 (17시도 × 7선거종류)
2. **stage 2** — 후보자별 공약 수집 (sg_type 3/4/11만)
3. **stage 3** — info.nec.go.kr PDF 다운로드 + 텍스트 추출 (pdfplumber → OCR fallback)
4. **stage 4** — Parquet 변환 + 시도별 파티셔닝

체크포인트: SQLite `data/checkpoint.db` — 단계별 resume 가능.

## info.nec.go.kr (정보공개자료) — 미해결 항목
- JSF 기반 페이지라 단순 GET 으론 PDF 못 받을 가능성 있음.
- `processor.discover_pdf_urls()` 의 정규식이 실제 페이지에서 동작하는지
  **8회 지선(sgId=20220601) 후보자로 사전 검증 필수.**
- 안 되면 playwright 도입.

## MCP 서버
- 트랜스포트: Streamable HTTP (FastMCP)
- 쿼리 엔진: DuckDB on Parquet (메모리 ~200MB)
- 도구: `search_candidates`, `get_candidate_detail`, `list_by_district`,
        `compare_candidates`, `list_districts`, `get_dataset_info`
- 호스팅: Docker 컨테이너 (electionmcp.kr)

## ⚠️ 보안 주의
- `.env` 는 git 에 커밋하지 말 것 (`.gitignore` 확인).
- 키가 외부에 노출되었다면 즉시 data.go.kr → 마이페이지 → 인증키 재발급.
- MCP 서버 포트는 nginx 리버스 프록시 + 필요 시 인증 레이어 적용.

## 개발 명령어
```bash
# 1. 의존성 설치
pip install -e .

# 2. 전체 파이프라인
python -m pipeline.ingest all

# 3. 특정 단계만
python -m pipeline.ingest stage 1   # 후보자 인덱스
python -m pipeline.ingest stage 2   # 공약
python -m pipeline.ingest stage 3   # PDF + OCR
python -m pipeline.ingest stage 4   # Parquet export

# 4. HF 업로드
python -m pipeline.upload push

# 5. MCP 서버
python -m mcp_server.server
```
