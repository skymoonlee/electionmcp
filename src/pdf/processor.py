"""
후보자정보공개자료 PDF 다운로드 + 텍스트 추출.

info.nec.go.kr 의 후보자별 PDF (전과/재산/병역/학력/납세) 처리.
1. pdfplumber 로 텍스트 레이어 추출 시도
2. 실패 시 pdf2image + PaddleOCR fallback

⚠️ info.nec.go.kr 은 JSF 기반이라 단순 GET 으론 동작 안 할 가능성 있음.
   8회 지선 데이터로 사전 검증 필수.
"""
from __future__ import annotations

import asyncio
import io
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
import pdfplumber
from pdf2image import convert_from_bytes

log = logging.getLogger(__name__)

INFO_NEC_BASE = "https://info.nec.go.kr"

# 정보공개자료 종류 (gubun 코드 — info.nec.go.kr 의 JS 분석으로 확정)
DISCLOSURE_GUBUN = {
    "education":         "1",  # 학력
    "assets":            "2",  # 재산
    "tax":               "3",  # 납세
    "military":          "4",  # 병역
    "criminal":          "5",  # 전과
    "career_edu":        "6",  # 교육경력
    "career_political":  "8",  # 공직선거경력
}

# PaddleOCR 인스턴스는 무거워서 lazy 싱글톤
_ocr_instance = None


def _get_ocr():
    """PaddleOCR 싱글톤. 한국어 모델."""
    global _ocr_instance
    if _ocr_instance is None:
        from paddleocr import PaddleOCR
        _ocr_instance = PaddleOCR(
            lang="korean",
            use_angle_cls=True,
            show_log=False,
        )
    return _ocr_instance


@dataclass
class DisclosureDocument:
    """후보자 1명의 정보공개자료 묶음."""
    hubo_id: str
    election_id: str
    documents: dict[str, str] = field(default_factory=dict)  # 문서종류 → 텍스트
    source_urls: dict[str, str] = field(default_factory=dict)
    extraction_method: dict[str, str] = field(default_factory=dict)  # 'pdfplumber' or 'ocr'


class PDFProcessor:
    def __init__(
        self,
        cache_dir: str | Path,
        concurrency: int = 10,
        timeout: float = 60.0,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(concurrency)
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                "Referer": INFO_NEC_BASE,
            },
            follow_redirects=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    def _cache_path(self, hubo_id: str, doc_type: str) -> Path:
        return self.cache_dir / f"{hubo_id}_{doc_type}.pdf"

    async def fetch_pdf(self, url: str, hubo_id: str, doc_type: str) -> Optional[bytes]:
        """PDF 1개 다운로드. 캐시 hit 시 디스크에서 로드."""
        cache_path = self._cache_path(hubo_id, doc_type)
        if cache_path.exists():
            return cache_path.read_bytes()

        async with self.semaphore:
            try:
                r = await self.client.get(url)
                r.raise_for_status()
                content = r.content
                if not content.startswith(b"%PDF"):
                    log.warning("Not a PDF (hubo=%s, type=%s, url=%s)", hubo_id, doc_type, url)
                    return None
                cache_path.write_bytes(content)
                return content
            except Exception as e:
                log.warning("PDF fetch failed (hubo=%s, type=%s): %s", hubo_id, doc_type, e)
                return None

    def extract_text(self, pdf_bytes: bytes) -> tuple[str, str]:
        """텍스트 추출. 반환: (text, method) — method='pdfplumber'|'ocr'|'failed'."""
        # 1차: pdfplumber
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n".join(pages).strip()
                if len(text) >= 50:  # 의미 있는 텍스트면 채택
                    return text, "pdfplumber"
        except Exception as e:
            log.warning("pdfplumber 실패: %s", e)

        # 2차: OCR fallback
        try:
            import numpy as np
            images = convert_from_bytes(pdf_bytes, dpi=200)
            ocr = _get_ocr()
            all_text = []
            for img in images:
                arr = np.array(img)
                result = ocr.ocr(arr, cls=True)
                if result and result[0]:
                    page_text = "\n".join(line[1][0] for line in result[0])
                    all_text.append(page_text)
            return "\n".join(all_text), "ocr"
        except Exception as e:
            log.error("OCR 실패: %s", e)
            return "", "failed"

    async def process_candidate(
        self,
        hubo_id: str,
        election_id: str,
        pdf_urls: dict[str, list[str]],
    ) -> DisclosureDocument:
        """후보자 1명의 모든 PDF 처리.

        pdf_urls = {'criminal': [url1, url2], 'assets': [url1], ...}
        한 종류 안에 여러 PDF 페이지 → 텍스트 합치기.
        """
        doc = DisclosureDocument(hubo_id=hubo_id, election_id=election_id)

        # 모든 (doc_type, page_idx, url) 조합을 펼쳐서 동시 다운로드
        flat: list[tuple[str, int, str]] = []
        for doc_type, urls in pdf_urls.items():
            for i, url in enumerate(urls):
                flat.append((doc_type, i, url))

        results = await asyncio.gather(*[
            self.fetch_pdf(url, hubo_id, f"{doc_type}_p{i}")
            for doc_type, i, url in flat
        ])

        # doc_type 별로 묶어 텍스트 추출
        loop = asyncio.get_running_loop()
        per_type: dict[str, list[tuple[bytes | None, str]]] = {}
        for (doc_type, _, url), pdf_bytes in zip(flat, results):
            per_type.setdefault(doc_type, []).append((pdf_bytes, url))

        for doc_type, pages in per_type.items():
            doc.source_urls[doc_type] = ", ".join(url for _, url in pages)
            texts: list[str] = []
            methods: list[str] = []
            for pdf_bytes, _ in pages:
                if pdf_bytes is None:
                    methods.append("failed")
                    continue
                text, method = await loop.run_in_executor(None, self.extract_text, pdf_bytes)
                if text:
                    texts.append(text)
                methods.append(method)
            doc.documents[doc_type] = "\n\n".join(texts)
            # 페이지 별 method 합쳐서 표기 ('pdfplumber,ocr')
            doc.extraction_method[doc_type] = ",".join(sorted(set(methods)))
        return doc


def normalize_election_id(sg_id: str) -> str:
    """OpenAPI sgId('20260603') → info.nec.go.kr electionId('0020260603') 변환."""
    return sg_id if sg_id.startswith("00") else f"00{sg_id}"


async def discover_pdf_urls(
    client: httpx.AsyncClient, sg_id: str, hubo_id: str
) -> dict[str, list[str]]:
    """
    info.nec.go.kr 의 JSON API 로 후보자 정보공개자료 PDF URL 목록 조회.

    메커니즘 (info.nec.go.kr/common/js/search.js + candidate_detail_info.xhtml 분석):
        1. /electioninfo/candidate_detail_scanSearchJson.json 호출 (gubun 별)
           응답 body[i].FILEPATH 에 원본 파일경로 (보통 .tif)
        2. 확장자를 .PDF 로 교체 후 /unielec_pdf_file/ prefix 부착

    반환: {doc_type: [pdf_url_1, pdf_url_2, ...]}  — 한 종류 안에 여러 페이지 가능
    """
    election_id = normalize_election_id(sg_id)
    referer = (
        f"{INFO_NEC_BASE}/electioninfo/candidate_detail_info.xhtml"
        f"?electionId={election_id}&huboId={hubo_id}"
    )
    json_endpoint = f"{INFO_NEC_BASE}/electioninfo/candidate_detail_scanSearchJson.json"

    result: dict[str, list[str]] = {}
    for doc_type, gubun in DISCLOSURE_GUBUN.items():
        try:
            r = await client.get(
                json_endpoint,
                params={
                    "gubun": gubun,
                    "electionId": election_id,
                    "huboId": hubo_id,
                    "statementId": "CPRI03_candidate_scanSearch",
                },
                headers={"Referer": referer},
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("scanSearchJson 실패 (hubo=%s, gubun=%s): %s", hubo_id, gubun, e)
            continue

        header = data.get("jsonResult", {}).get("header", {})
        if header.get("result") != "ok":
            log.debug("scanSearchJson result=%s (hubo=%s, gubun=%s)",
                      header.get("result"), hubo_id, gubun)
            continue
        body = data.get("jsonResult", {}).get("body") or []
        urls: list[str] = []
        for item in body:
            file_path = item.get("FILEPATH", "")
            if not file_path:
                continue
            # 확장자 .tif/.jpg → .PDF 로 교체
            dot = file_path.rfind(".")
            base = file_path[:dot] if dot >= 0 else file_path
            urls.append(f"{INFO_NEC_BASE}/unielec_pdf_file/{base}.PDF")
        if urls:
            result[doc_type] = urls
    return result
