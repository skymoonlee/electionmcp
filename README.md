# electionmcp

> 2026년 6월 3일 제9회 전국동시지방선거.
> 내 지역구 후보 정보를, AI에게 그냥 물어볼 수 있게.

```
"강남구 시장 후보 누가 나왔어?"
"김해시 시의원 후보들 공약 비교해줘."
"성남 분당구 교육감 후보 학력이랑 경력 알려줘."
```

Claude·ChatGPT 같은 AI에 물으면, 중앙선거관리위원회 공식 데이터를 바탕으로 답한다.
모든 답에 **출처가 함께 표시**된다.

🌐 **MCP 엔드포인트**: `https://mcp.electionmcp.kr/mcp`

---

## 왜 만들었는가

지방선거 한 번에 한 사람이 4~7명의 대표를 뽑는다 — 시·도지사, 구·시·군장,
광역의원, 기초의원, 교육감. 후보자 정보는 중앙선관위가 모두 공개하고 있지만,
여러 사이트에 흩어져 있고, 대부분 PDF 안에 갇혀 있고, 한눈에 비교하기 어렵다.

그래서 투표소에 가서야 처음 후보 이름을 보는 일이 흔하다.

**electionmcp 은 흩어진 공식 데이터를 한 곳에 모으고, AI가 자연어로 답할 수 있게
정리하는 프로젝트다.** 유권자가 자기 지역구 후보를 누구인지, 어떤 사람인지,
공약은 무엇인지 — **객관적 사실만으로** 충분히 알아본 다음 투표소에 갈 수 있게.

## 원칙

| | |
|---|---|
| ✅ **하는 것** | 사실 정보 조회·비교·정리 |
| ✅ **하는 것** | 모든 응답에 출처(중앙선관위) + 원본 URL 표시 |
| ✅ **하는 것** | 후보자 본인이 공식 등록한 정보 그대로 전달 |
| ❌ **안 하는 것** | 후보 추천·평가·우열 비교 |
| ❌ **안 하는 것** | 정파적 해석, 지지율·여론조사 가공 |
| ❌ **안 하는 것** | 데이터 변형·요약·왜곡 |

> 판단은 유권자의 몫이다. 이 도구는 그 판단에 필요한 정보만 제공한다.

## 어떻게 쓰나

### Claude Desktop / Claude Code

`~/.claude.json` 또는 `claude_desktop_config.json` 에 추가:

```json
{
  "mcpServers": {
    "electionmcp": {
      "type": "http",
      "url": "https://mcp.electionmcp.kr/mcp"
    }
  }
}
```

Claude 재시작 후, 자유롭게 질문하면 된다.

### Cursor / Cline / 기타 MCP 클라이언트

같은 URL로 Streamable HTTP 트랜스포트로 연결.

### 제공 도구

| 도구 | 용도 |
|---|---|
| `search_candidates` | 시도/시군구/정당/이름으로 후보자 검색 |
| `list_by_district` | 특정 선거구의 모든 후보자 |
| `get_candidate_detail` | 후보자 1명의 전체 정보 (공약 + 정보공개자료 포함) |
| `compare_candidates` | 후보자 N명을 같은 항목 기준으로 비교 |
| `list_districts` | 시도/시군구 목록 |
| `get_dataset_info` | 데이터셋 메타정보 (마지막 갱신 시각 등) |

## 데이터 출처

모든 데이터는 **중앙선거관리위원회**에서 가져온다.

| 출처 | 제공 내용 | 라이선스 |
|---|---|---|
| [공공데이터포털 OpenAPI](https://www.data.go.kr) | 후보자 인적사항, 학력, 경력, 공약 | 공공누리 제1유형 |
| [info.nec.go.kr](http://info.nec.go.kr) | 전과기록, 재산신고, 병역, 납세 | 공직선거법 §49 공개자료 |

법적 근거: **공직선거법 제49조** — 후보자정보 공개 제도(국민의 알권리·선거권 행사 보장).

## 투명성

| 보장 메커니즘 | 효과 |
|---|---|
| 코드 100% 공개 (이 저장소) | 데이터 가공 로직을 누구나 직접 검증 |
| HuggingFace Dataset 공개 | 가공된 전체 데이터를 누구나 다운로드해 검증 |
| 모든 응답에 원본 URL 명시 | NEC 공식 자료와 1:1 대조 가능 |
| MCP 응답 평가/추천 차단 | 시스템 프롬프트로 강제 |

## 자체 호스팅

직접 서버를 운영하거나, 코드를 수정해 사용하고 싶다면:

```bash
git clone https://github.com/skymoonlee/electionmcp
cd electionmcp
cp env.example .env
# .env 열어 NEC_API_KEY, HF_TOKEN 채우기

pip install -e .
python -m pipeline.ingest all       # 데이터 수집
python -m pipeline.upload push      # HuggingFace 업로드 (선택)
python -m mcp_server.server         # MCP 서버 기동 (port 8780)
```

Docker:
```bash
docker compose up -d mcp
docker compose --profile manual run --rm ingest
```

NEC OpenAPI 인증키는 [공공데이터포털](https://www.data.go.kr)에서 무료 발급
(자동승인, 5분 소요).

## 한계

- 본 서비스는 **정보 제공 도구**이지 투표 안내가 아니다.
- AI 응답은 NEC 데이터 기반이지만, 자연어 생성 과정에서 부정확할 수 있다.
  중요한 결정 전에는 [info.nec.go.kr](http://info.nec.go.kr) 원문 재확인 권장.
- 선거 6일 전(2026.5.29 ~)부터 여론조사 결과 공표 금지(공직선거법 §108).
  본 서비스는 애초에 여론조사·지지율 정보를 제공하지 않는다.
- 6월 3일 선거일 이후 NEC가 정보공개자료를 비공개 처리한다. 본 서비스의
  데이터 갱신도 같은 날 중단된다.

## 기여

이슈·PR 환영.

- **데이터 오류 발견** → Issues 에 신고. 단, 정정은 NEC가 원본을 수정한 뒤에야 반영 가능.
- **새 기능 / 개선** → PR.
- **법적 문의 / 우려** → Issues 또는 직접 연락.

## 라이선스

- 코드: [MIT](./LICENSE)
- 데이터: 공공누리 제1유형 (출처: 중앙선거관리위원회)

---

> *이 프로젝트의 유일한 목적은 유권자의 알권리와 선거권 행사 보장이다.*
> *(공직선거법 제49조)*
