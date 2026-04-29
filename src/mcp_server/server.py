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

mcp = FastMCP(
    "korea-election-2026",
    instructions=(
        "9회 전국동시지방선거 (2026.6.3) 후보자 정보 제공 서버.\n"
        "유권자가 자신의 지역구 후보자를 조회·비교할 수 있게 돕습니다.\n"
        "정보 제공만 하며 후보 추천이나 평가 의견은 제시하지 않습니다.\n"
        "출처: 중앙선거관리위원회 공공데이터포털"
    ),
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


@mcp.tool()
def get_candidate_detail(hubo_id: str) -> dict[str, Any]:
    """후보자 1명 전체 정보 (공약 + 정보공개자료 포함)."""
    df = get_db().execute(
        "SELECT * FROM candidates WHERE hubo_id = ?", [hubo_id]
    ).df()
    if df.empty:
        return {"error": f"hubo_id={hubo_id} 후보자 없음"}
    row = df.iloc[0].to_dict()
    row.pop("raw_json", None)
    row["_source"] = "중앙선거관리위원회"
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
    """후보자 여러 명을 같은 항목 기준으로 비교 (2-5명 권장)."""
    if len(hubo_ids) < 2:
        return {"error": "최소 2명 이상 지정"}
    placeholders = ",".join(["?"] * len(hubo_ids))
    df = get_db().execute(
        f"SELECT * FROM candidates WHERE hubo_id IN ({placeholders})",
        hubo_ids,
    ).df()
    if df.empty:
        return {"error": "후보자 없음"}
    df = df.drop(columns=["raw_json"], errors="ignore")
    return {
        "candidates": df.to_dict("records"),
        "comparison_fields": [
            "name", "party", "ballot_number", "pledges",
            "criminal", "assets", "military", "education", "tax",
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
