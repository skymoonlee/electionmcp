"""
MCP 서버 - 9회 지방선거 후보자 정보 조회.

FastMCP + Streamable HTTP 트랜스포트.
DuckDB 로 Parquet 직접 쿼리 (메모리 ~200MB, 응답 ms 단위).

도구:
- search_candidates: 시도/시군구/정당/이름으로 후보자 검색
- get_candidate_detail: 후보자 1명 전체 정보
- list_by_district: 특정 선거구의 모든 후보자
- compare_candidates: 후보자 N명 비교
- list_districts: 시도/시군구 목록
- get_dataset_info: 데이터셋 메타정보 (갱신 시각 등)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

import duckdb
from datasets import load_dataset
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

load_dotenv()

# ---------- 데이터 로드 ----------

DATASET_REPO = os.getenv("HF_DATASET_REPO", "electionmcp/korea-local-election-2026")
LOCAL_PARQUET = os.getenv("LOCAL_PARQUET", "./data/parquet/all.parquet")

_db: Optional[duckdb.DuckDBPyConnection] = None
_data_source: str = "unknown"
_loaded_at: Optional[str] = None


def get_db() -> duckdb.DuckDBPyConnection:
    """DuckDB 싱글톤. 로컬 Parquet 우선, 없으면 HF에서 로드."""
    global _db, _data_source, _loaded_at
    if _db is not None:
        return _db

    from datetime import datetime, timezone

    _db = duckdb.connect(":memory:")
    if Path(LOCAL_PARQUET).exists():
        _db.execute(f"CREATE VIEW candidates AS SELECT * FROM '{LOCAL_PARQUET}'")
        _data_source = f"local:{LOCAL_PARQUET}"
    else:
        ds = load_dataset(DATASET_REPO, split="all")
        df = ds.to_pandas()
        _db.execute("CREATE TABLE candidates AS SELECT * FROM df")
        _data_source = f"huggingface:{DATASET_REPO}"
    _loaded_at = datetime.now(timezone.utc).isoformat()
    return _db


# ---------- MCP 서버 ----------

def _build_transport_security():
    """리버스 프록시 환경에서 외부 도메인을 허용하도록 trusted hosts 설정.
    환경변수 ALLOWED_HOSTS 로 추가 (콤마 구분).
    """
    from mcp.server.transport_security import TransportSecuritySettings
    base_hosts = ["127.0.0.1:*", "localhost:*", "[::1]:*"]
    base_origins = ["http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"]
    extra_hosts = [h.strip() for h in os.getenv("ALLOWED_HOSTS", "").split(",") if h.strip()]
    extra_origins: list[str] = []
    for h in extra_hosts:
        # http/https 양쪽 다 허용
        extra_origins += [f"https://{h}", f"http://{h}"]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=base_hosts + extra_hosts,
        allowed_origins=base_origins + extra_origins,
    )


mcp = FastMCP(
    "korea-election-2026",
    instructions=(
        "대한민국 제9회 전국동시지방선거 (2026.6.3) 후보자 정보 제공 MCP 서버.\n"
        "출처: 중앙선거관리위원회 공공데이터포털 + info.nec.go.kr.\n"
        "\n"
        "[응답 원칙]\n"
        "- 후보자 정보를 답할 때는 도구가 반환한 모든 가용 필드를\n"
        "  최대한 빠짐없이 정리해서 사용자에게 제공한다.\n"
        "  (이름·정당·기호·생년월일·나이·성별·주소·직업·학력·경력 1~3·\n"
        "   등록상태·공약·전과·재산·병역·납세·체납 등)\n"
        "- 필드가 비어 있거나 누락된 경우 '미공개' 또는 '해당 단계 미제공'\n"
        "  이라고 명시한다. 임의로 추정·보완하지 말 것.\n"
        "- 모든 응답에 출처 (중앙선거관리위원회) 를 명시한다.\n"
        "- 후보 추천·우열 평가·정파적 해석은 일체 하지 않는다.\n"
        "  사실 정보 제공·비교·요약만 한다.\n"
        "\n"
        "[도구 선택 가이드]\n"
        "- 사용자가 '내 지역', '우리 동네' 등 모호하게 묻거나 시·도/시·군·구가\n"
        "  불명확할 때는 먼저 `list_districts` 로 후보 시·도·시·군·구를 보여준다.\n"
        "- 특정 선거구의 모든 후보를 한 번에 보고 싶을 때는 `list_by_district`.\n"
        "- 이름·정당·키워드 검색은 `search_candidates`.\n"
        "  정당은 부분일치이므로 '민주' → 더불어민주당·민주노동당 등 모두 매칭됨.\n"
        "- 후보자 1인 상세는 `get_candidate_detail` (가용한 모든 필드 반환).\n"
        "- 2~5명 비교는 `compare_candidates`.\n"
        "\n"
        "[데이터 가용 시점 — AI 가 사용자에게 알려야 함]\n"
        "- 5/14~15 정식등록 이전: 예비후보 인적정보만 가용.\n"
        "  공약·전과·재산·병역 등 정보공개자료는 NEC가 아직 받지 않아 미제공.\n"
        "- 5/16 ~ 6/3: 정식후보 + 공약 + 정보공개자료 모두 가용.\n"
        "- 6/4 이후: 스냅샷. info.nec.go.kr 정보공개자료는 NEC 정책에 따라\n"
        "  비공개 전환되며, 본 서버는 6/3까지 수집한 자료를 그대로 보존.\n"
        "\n"
        "[지역 표기 주의사항]\n"
        "- 9회 지선부터 광주광역시 + 전라남도가 '전남광주통합특별시' 로 통합 운영.\n"
        "  사용자가 '광주' 또는 '전남' 으로 물어도 통합특별시 후보를 함께 안내.\n"
    ),
    transport_security=_build_transport_security(),
)


@mcp.tool()
def search_candidates(
    sido: Optional[str] = None,
    sgg: Optional[str] = None,
    party: Optional[str] = None,
    name: Optional[str] = None,
    sg_type: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    후보자 검색.

    Args:
        sido: 광역시도 (예: '경상남도', '서울특별시')
        sgg: 시군구 (예: '김해시', '강남구')
        party: 정당명 일부 (예: '민주', '국민의힘')
        name: 후보자 이름 일부
        sg_type: 선거 종류 코드 (3=시도지사, 4=시군구청장, 5/6=지역구의원, 11=교육감)
        limit: 최대 결과 수
    """
    where, params = [], []
    if sido:
        where.append("sido = ?"); params.append(sido)
    if sgg:
        where.append("sgg = ?"); params.append(sgg)
    if party:
        where.append("party LIKE ?"); params.append(f"%{party}%")
    if name:
        where.append("name LIKE ?"); params.append(f"%{name}%")
    if sg_type:
        where.append("sg_type = ?"); params.append(sg_type)

    where_clause = " AND ".join(where) if where else "1=1"
    sql = f"""
        SELECT hubo_id, name, party, ballot_number,
               sg_type_name, sido, sgg, district
        FROM candidates
        WHERE {where_clause}
        ORDER BY sido, sgg, sg_type, ballot_number
        LIMIT ?
    """
    params.append(limit)
    df = get_db().execute(sql, params).df()
    return df.to_dict("records")


_RAW_FIELD_MAP = {
    # NEC raw API 필드명 → 사용자 친화 키
    "hanjaName": "name_hanja",
    "gender": "gender",
    "birthday": "birthday",
    "age": "age",
    "addr": "address",
    "job": "job",
    "edu": "education",
    "career1": "career_1",
    "career2": "career_2",
    "career3": "career_3",
    "career4": "career_4",
    "career5": "career_5",
    "regdate": "registration_date",
    "status": "registration_status",
}


def _flatten_candidate_row(row: dict) -> dict:
    """raw_json 안에 묻혀 있던 학력·경력·인적사항을 top-level 로 풀어 반환."""
    raw_json = row.pop("raw_json", None)
    if isinstance(raw_json, str) and raw_json:
        import json as _json
        try:
            raw = _json.loads(raw_json)
        except _json.JSONDecodeError:
            raw = {}
    elif isinstance(raw_json, dict):
        raw = raw_json
    else:
        raw = {}
    for raw_key, friendly_key in _RAW_FIELD_MAP.items():
        if raw_key in raw and raw[raw_key] not in (None, ""):
            row[friendly_key] = raw[raw_key]
    return row


@mcp.tool()
def get_candidate_detail(hubo_id: str) -> dict[str, Any]:
    """후보자 1명 전체 정보.

    공약·정보공개자료(전과·재산·병역·납세·학력)는 정식등록(2026-05-15) 이후에
    채워진다. 그 이전에는 인적사항·정당·학력 1줄·경력만 가용하며, 비어 있는
    항목은 응답 dict 에서 제외된다.
    """
    df = get_db().execute(
        "SELECT * FROM candidates WHERE hubo_id = ?", [hubo_id]
    ).df()
    if df.empty:
        return {"error": f"hubo_id={hubo_id} 후보자 없음"}
    row = _flatten_candidate_row(df.iloc[0].to_dict())
    row["_source"] = "중앙선거관리위원회"
    row["_source_url"] = (
        f"https://info.nec.go.kr/electioninfo/candidate_detail_info.xhtml"
        f"?electionId=00{row.get('sg_id','')}&huboId={hubo_id}"
    )
    return row


@mcp.tool()
def list_by_district(sido: str, sgg: Optional[str] = None) -> dict[str, list[dict]]:
    """
    선거구별 후보자 전체 목록 (선거 종류별로 그룹화).

    Args:
        sido: 광역시도 (필수)
        sgg: 시군구 (선택, 없으면 광역단체장+교육감만)
    """
    where = ["sido = ?"]
    params = [sido]
    if sgg:
        where.append("(sgg = ? OR sg_type IN ('3', '11'))")
        params.append(sgg)

    sql = f"""
        SELECT hubo_id, name, party, ballot_number,
               sg_type, sg_type_name, sgg, district
        FROM candidates
        WHERE {" AND ".join(where)}
        ORDER BY sg_type, sgg, ballot_number
    """
    df = get_db().execute(sql, params).df()
    grouped: dict[str, list[dict]] = {}
    for sg_type_name, group in df.groupby("sg_type_name"):
        grouped[sg_type_name] = group.drop(columns=["sg_type_name"]).to_dict("records")
    return grouped


@mcp.tool()
def compare_candidates(hubo_ids: list[str]) -> dict[str, Any]:
    """후보자 여러 명을 같은 항목 기준으로 비교 (2-5명 권장).

    각 후보의 인적사항(학력·경력 포함) + 공약 + 정보공개자료를 모두 펼쳐서
    같은 키 구조로 반환한다. 5/15 정식등록 전에는 공약·정보공개자료 항목이
    비어 있다.
    """
    if len(hubo_ids) < 2:
        return {"error": "최소 2명 이상 지정"}
    placeholders = ",".join(["?"] * len(hubo_ids))
    df = get_db().execute(
        f"SELECT * FROM candidates WHERE hubo_id IN ({placeholders})",
        hubo_ids,
    ).df()
    if df.empty:
        return {"error": "후보자 없음"}
    candidates = [_flatten_candidate_row(r) for r in df.to_dict("records")]
    return {
        "candidates": candidates,
        "comparison_fields": [
            "name", "party", "ballot_number",
            "education", "career_1", "career_2", "career_3",
            "job", "age", "registration_status",
            "pledges", "criminal", "assets", "military", "tax",
        ],
        "_source": "중앙선거관리위원회",
    }


@mcp.tool()
def list_districts(sido: Optional[str] = None) -> list[dict[str, Any]]:
    """
    시도/시군구 목록 (후보자 수 포함).

    Args:
        sido: 광역시도 (없으면 전체 시도, 있으면 해당 시도의 시군구)
    """
    if sido:
        sql = """
            SELECT sgg, COUNT(*) AS candidate_count
            FROM candidates
            WHERE sido = ? AND sgg IS NOT NULL
            GROUP BY sgg
            ORDER BY sgg
        """
        df = get_db().execute(sql, [sido]).df()
    else:
        sql = """
            SELECT sido, COUNT(*) AS candidate_count
            FROM candidates
            WHERE sido IS NOT NULL
            GROUP BY sido
            ORDER BY sido
        """
        df = get_db().execute(sql).df()
    return df.to_dict("records")


@mcp.tool()
def get_dataset_info() -> dict[str, Any]:
    """데이터셋 메타정보 (소스, 로딩 시각, 총 후보자 수)."""
    db = get_db()
    counts = db.execute(
        "SELECT COUNT(*) AS total, COUNT(DISTINCT sido) AS sido_count FROM candidates"
    ).fetchone()
    return {
        "data_source": _data_source,
        "loaded_at_utc": _loaded_at,
        "total_candidates": counts[0],
        "sido_count": counts[1],
        "election": "9회 전국동시지방선거",
        "election_date": "2026-06-03",
        "license": "공공누리 제1유형 (출처표시)",
        "source_attribution": "중앙선거관리위원회",
    }


# ---------- 헬스체크 (FastMCP custom route) ----------

@mcp.custom_route("/health", methods=["GET"])
async def healthcheck(request: Request):
    try:
        db = get_db()
        n = db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
        return JSONResponse({"status": "ok", "candidates": n, "source": _data_source})
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=503)


# ---------- 엔트리포인트 ----------

def main():
    """HTTP 트랜스포트로 MCP 서버 실행."""
    host = os.getenv("MCP_HOST", "0.0.0.0")
    port = int(os.getenv("MCP_PORT", "8780"))

    get_db()  # 워밍업
    print(f"MCP server starting on http://{host}:{port}/mcp (health: /health)")

    mcp.settings.host = host
    mcp.settings.port = port
    mcp.run(transport="streamable-http")


if __name__ == "__main__":
    main()
