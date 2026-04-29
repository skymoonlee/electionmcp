# Korea Election MCP — 9회 전국동시지방선거 (2026.6.3)

중앙선관위 OpenAPI + 후보자정보공개자료(PDF)를 수집해 HuggingFace Dataset으로 배포하고,
MCP 서버를 통해 유권자가 자기 지역구 후보 정보를 조회·비교할 수 있게 한다.

## 디렉터리 구조
```
.
├── pyproject.toml
├── .env                     # 실제 키 (git ignore)
├── env.example              # 템플릿
├── CLAUDE.md                # 프로젝트 컨텍스트
├── Dockerfile
├── docker-compose.yml
└── src/
    ├── nec/client.py        # NEC OpenAPI 비동기 클라이언트
    ├── pdf/processor.py     # PDF 다운로드 + pdfplumber + PaddleOCR fallback
    ├── pipeline/
    │   ├── ingest.py        # 4단계 수집 파이프라인
    │   └── upload.py        # HuggingFace Hub 업로드
    └── mcp_server/server.py # FastMCP HTTP 서버
```

## 활용신청한 NEC OpenAPI

| 데이터명 | 엔드포인트 |
|---|---|
| 중앙선거관리위원회_코드 정보 | `CommonCodeService` |
| 중앙선거관리위원회_선거공약 정보 | `ElecPrmsInfoInqireService` |
| 중앙선거관리위원회_후보자 정보 | `PofelcddInfoInqireService` |

베이스 URL: `https://apis.data.go.kr/9760000` (HTTPS 필수)

## 빠른 시작

```bash
# 1. 의존성
pip install -e .

# 2. 키 검증 — 9회 지선 sgId 확정
python -m pipeline.ingest verify-sgid

# 3. 8회 지선(2022.6.1) 데이터로 sanity check
SG_ID=20220601 CANDIDATE_STAGE=official python -m pipeline.ingest stage 1

# 4. 실제 9회 지선 풀 파이프라인 (5/15 정식등록 이후)
python -m pipeline.ingest all

# 5. HF 업로드
python -m pipeline.upload push

# 6. MCP 서버
python -m mcp_server.server
```

## Docker

```bash
# MCP 24/7 가동
docker compose up -d mcp

# ingest 1회 수동 실행
docker compose --profile manual run --rm ingest

# 헬스체크
curl http://localhost:8765/health
```

리버스 프록시 (nginx) 예시:
```nginx
location /mcp/ {
    proxy_pass http://localhost:8765/mcp/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_buffering off;
    proxy_read_timeout 3600s;
}
```

## 9회 지선 타임라인
- **~5/14**: 예비후보자 (`CANDIDATE_STAGE=preliminary`)
- **5/15-16**: 정식 후보자 등록 → 첫 풀스캔
- **5/17 ~ 6/2**: 매일 갱신 (`CANDIDATE_STAGE=official`)
- **5/29 ~**: 여론조사 결과 공표 금지 (§108)
- **6/3**: 선거일
- **6/3 이후**: NEC가 정보공개 PDF 비공개 처리, 미리 받아둔 데이터만 사용

권장 실행 스케줄:
```
5/16 22:00 → 정식 후보자 풀스캔 (stage 1, 2)
5/17 02:00 → PDF + OCR 수집 (stage 3)
5/17 06:00 → Parquet export + HF push (stage 4 + upload)
이후 매일 02:00 → 갱신
```

## MCP 도구

- `search_candidates(sido, sgg, party, name, sg_type, limit)` — 검색
- `get_candidate_detail(hubo_id)` — 후보자 상세
- `list_by_district(sido, sgg)` — 선거구별 모든 후보
- `compare_candidates(hubo_ids)` — 후보 비교
- `list_districts(sido)` — 시도/시군구 목록
- `get_dataset_info()` — 데이터셋 메타정보

## ⚠️ PDF URL 패턴 검증 필수

`pdf/processor.py` 의 `discover_pdf_urls()` 함수는 `info.nec.go.kr` 의 detail 페이지에서
PDF 링크를 정규식으로 추출함. JSF 기반이라 단순 GET 으로 안 될 가능성이 있으니
**배포 전 8회 지선 데이터로 검증 필수**:

```python
import asyncio, httpx
from pdf.processor import discover_pdf_urls

async def test():
    async with httpx.AsyncClient(follow_redirects=True) as client:
        # 8회 지선 후보자 ID 하나로 테스트
        urls = await discover_pdf_urls(client, "20220601", "<huboId>")
        print(urls)

asyncio.run(test())
```

안 되면 playwright 도입 필요.

## 라이선스 + 이용 정책

- 코드: MIT
- 데이터: 중앙선관위 공공누리 1유형 (출처표시)
- **목적 제한**: 공직선거법 제49조에 따른 유권자 정보 제공 목적으로만 사용
- 후보자 평가/추천은 시스템 차원에서 금지 (MCP 서버 instruction 으로 명시)
