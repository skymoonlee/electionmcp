"""
NEC OpenAPI 클라이언트.

공공데이터포털 (data.go.kr) 의 중앙선관위 API들을 비동기로 호출.
- 후보자 정보       (PofelcddInfoInqireService)
- 선거공약 정보     (ElecPrmsInfoInqireService)
- 코드 정보         (CommonCodeService)

레이트 리밋: 개발계정 10,000/일/엔드포인트.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Literal

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = logging.getLogger(__name__)

BASE_URL = "https://apis.data.go.kr/9760000"

# 9회 전국동시지방선거 선거종류코드 (NEC CommonCodeService 기준 검증)
# 1: 대통령, 2: 국회의원, 3: 시·도지사, 4: 구·시·군의장,
# 5: 시·도의원(지역구), 6: 구·시·군의원(지역구),
# 8: 광역의원비례대표, 9: 기초의원비례대표,
# 11: 교육감
SG_TYPES_LOCAL_ELECTION = {
    "3": "시·도지사",
    "4": "구·시·군의장",
    "5": "시·도의원(지역구)",
    "6": "구·시·군의원(지역구)",
    "8": "광역의원비례대표",
    "9": "기초의원비례대표",
    "11": "교육감",
}

# 공약서 제출 의무 있는 선거종류만 공약 API 조회 가능
SG_TYPES_WITH_PLEDGES = {"3", "4", "11"}

CandidateStage = Literal["preliminary", "official"]


class NECClient:
    """비동기 NEC OpenAPI 클라이언트."""

    def __init__(
        self,
        service_key: str,
        rps: int = 5,
        timeout: float = 30.0,
    ):
        self.service_key = service_key
        self.semaphore = asyncio.Semaphore(rps)
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    async def _request(self, path: str, params: dict[str, Any]) -> dict:
        params = {
            "serviceKey": self.service_key,
            "resultType": "json",
            **params,
        }
        async with self.semaphore:
            r = await self.client.get(f"{BASE_URL}{path}", params=params)
            r.raise_for_status()
            data = r.json()
            # NEC API는 200 OK여도 body 안에 에러를 넣어 보내는 경우가 있음.
            # 성공 코드는 엔드포인트마다 다름: '00', 'INFO-00' 모두 정상 처리 의미.
            # 'INFO-200' 류는 "데이터 없음" — 빈 응답으로 취급 (에러 아님).
            header = data.get("response", {}).get("header", {})
            code = header.get("resultCode") or ""
            if code and code != "00" and not code.startswith("INFO-"):
                raise RuntimeError(f"NEC API error {code}: {header.get('resultMsg')}")
            return data

    @staticmethod
    def _extract_items(data: dict) -> list[dict]:
        items = data.get("response", {}).get("body", {}).get("items", {})
        if isinstance(items, dict):
            items = items.get("item", [])
        if isinstance(items, dict):  # 단일 결과는 dict로 옴
            items = [items]
        return items or []

    @staticmethod
    def _extract_total(data: dict) -> int:
        body = data.get("response", {}).get("body", {})
        try:
            return int(body.get("totalCount", 0))
        except (TypeError, ValueError):
            return 0

    # ---------- 후보자 정보 ----------
    # 예비후보자: /getPoelpcddRegistSttusInfoInqire (Poelpcdd)
    # 정식후보자: /getPofelcddRegistSttusInfoInqire (Pofelcdd)
    # ⚠️ 비슷해 보여도 다른 엔드포인트.

    async def get_candidates_page(
        self,
        sg_id: str,
        sg_type: str,
        page: int = 1,
        num_rows: int = 100,
        stage: CandidateStage = "official",
    ) -> dict:
        if stage == "preliminary":
            path = "/PofelcddInfoInqireService/getPoelpcddRegistSttusInfoInqire"
        else:
            path = "/PofelcddInfoInqireService/getPofelcddRegistSttusInfoInqire"
        return await self._request(
            path,
            {
                "pageNo": page,
                "numOfRows": num_rows,
                "sgId": sg_id,
                "sgTypecode": sg_type,
            },
        )

    async def iter_candidates(
        self,
        sg_id: str,
        sg_type: str,
        num_rows: int = 100,
        stage: CandidateStage = "official",
    ) -> AsyncIterator[dict]:
        """선거ID + 선거종류코드로 모든 후보자 페이지 순회."""
        page = 1
        while True:
            data = await self.get_candidates_page(sg_id, sg_type, page, num_rows, stage)
            items = self._extract_items(data)
            if not items:
                break
            for item in items:
                yield item
            total = self._extract_total(data)
            if page * num_rows >= total:
                break
            page += 1

    # ---------- 선거공약 ----------

    async def get_pledges(
        self, sg_id: str, sg_type: str, hubo_id: str
    ) -> list[dict]:
        """후보자 1명의 공약 리스트."""
        if sg_type not in SG_TYPES_WITH_PLEDGES:
            return []
        try:
            data = await self._request(
                "/ElecPrmsInfoInqireService/getCnddtElecPrmsInfoInqire",
                {
                    "sgId": sg_id,
                    "sgTypecode": sg_type,
                    "cnddtId": hubo_id,
                    "numOfRows": 100,
                    "pageNo": 1,
                },
            )
        except Exception as e:
            log.warning("공약 조회 실패 (hubo_id=%s): %s", hubo_id, e)
            return []
        return self._extract_items(data)

    # ---------- 코드 정보 ----------

    async def get_election_codes(self, num_rows: int = 100) -> list[dict]:
        """선거 ID 목록 조회. 9회 지선 sgId 확정용."""
        data = await self._request(
            "/CommonCodeService/getCommonSgCodeList",
            {"numOfRows": num_rows, "pageNo": 1},
        )
        return self._extract_items(data)

    async def get_gusigun_codes(self, sg_id: str) -> list[dict]:
        """전체 시도의 시군구 코드 조회."""
        data = await self._request(
            "/CommonCodeService/getCommonGusigunCodeList",
            {"numOfRows": 1000, "pageNo": 1, "sgId": sg_id},
        )
        return self._extract_items(data)

    async def get_sgg_codes(self, sg_id: str, sg_type: str) -> list[dict]:
        """선거구 코드 + 선출정수 조회."""
        data = await self._request(
            "/CommonCodeService/getCommonSggCodeList",
            {
                "numOfRows": 1000,
                "pageNo": 1,
                "sgId": sg_id,
                "sgTypecode": sg_type,
            },
        )
        return self._extract_items(data)

    async def get_party_codes(self, sg_id: str) -> list[dict]:
        """정당 코드 조회."""
        data = await self._request(
            "/CommonCodeService/getCommonPartyCodeList",
            {"numOfRows": 500, "pageNo": 1, "sgId": sg_id},
        )
        return self._extract_items(data)

    async def get_job_codes(self, sg_id: str) -> list[dict]:
        """직업 코드 조회."""
        data = await self._request(
            "/CommonCodeService/getCommonJobCodeList",
            {"numOfRows": 500, "pageNo": 1, "sgId": sg_id},
        )
        return self._extract_items(data)

    async def get_edu_codes(self, sg_id: str) -> list[dict]:
        """학력 코드 조회."""
        data = await self._request(
            "/CommonCodeService/getCommonEduBckgrdCodeList",
            {"numOfRows": 500, "pageNo": 1, "sgId": sg_id},
        )
        return self._extract_items(data)
