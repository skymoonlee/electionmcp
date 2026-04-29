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
        self, hubo_id: str, election_id: str, pdf_urls: dict[str, str]
    ) -> DisclosureDocument:
        """후보자 1명의 모든 PDF 처리. pdf_urls = {'criminal': url, 'assets': url, ...}"""
        doc = DisclosureDocument(hubo_id=hubo_id, election_id=election_id)
        # 동시 다운로드
        results = await asyncio.gather(*[
            self.fetch_pdf(url, hubo_id, doc_type)
            for doc_type, url in pdf_urls.items()
        ])
        # 텍스트 추출은 CPU bound이므로 thread pool로
        loop = asyncio.get_running_loop()
        for (doc_type, url), pdf_bytes in zip(pdf_urls.items(), results):
            doc.source_urls[doc_type] = url
            if pdf_bytes is None:
                doc.documents[doc_type] = ""
                doc.extraction_method[doc_type] = "failed"
                continue
            text, method = await loop.run_in_executor(None, self.extract_text, pdf_bytes)
            doc.documents[doc_type] = text
            doc.extraction_method[doc_type] = method
        return doc


def build_disclosure_urls(election_id: str, hubo_id: str) -> dict[str, str]:
    """
    후보자 정보공개자료 PDF URL 추정 (정적 패턴).

    ⚠️ 실제 URL 패턴은 info.nec.go.kr 페이지를 한 번 inspect해서 확정해야 함.
    아래는 일반적으로 관찰되는 패턴이며, 실제 적용 전에 1-2개 후보로 테스트 필수.
    """
    base = f"{INFO_NEC_BASE}/electioninfo/cnddt_pdf"
    return {
        "criminal": f"{base}/{election_id}/{hubo_id}/criminal.pdf",
        "assets": f"{base}/{election_id}/{hubo_id}/assets.pdf",
        "military": f"{base}/{election_id}/{hubo_id}/military.pdf",
        "education": f"{base}/{election_id}/{hubo_id}/education.pdf",
        "tax": f"{base}/{election_id}/{hubo_id}/tax.pdf",
    }


_PDF_LINK_RE = re.compile(r'(?:src|href)="([^"]+\.pdf[^"]*)"', re.IGNORECASE)
_KEYWORD_MAP = {
    "criminal": ["criminal", "전과", "범죄"],
    "assets": ["asset", "재산"],
    "military": ["military", "병역"],
    "education": ["edu", "학력"],
    "tax": ["tax", "납세", "체납"],
}


async def discover_pdf_urls(
    client: httpx.AsyncClient, election_id: str, hubo_id: str
) -> dict[str, str]:
    """
    Detail 페이지를 fetch해서 실제 PDF URL을 동적으로 추출.
    build_disclosure_urls() 가 안 맞으면 이 함수를 사용.
    """
    detail_url = (
        f"{INFO_NEC_BASE}/electioninfo/candidate_detail_info.xhtml"
        f"?electionId={election_id}&huboId={hubo_id}"
    )
    try:
        r = await client.get(detail_url)
        r.raise_for_status()
    except Exception as e:
        log.warning("Detail page fetch 실패 (hubo=%s): %s", hubo_id, e)
        return {}
    html = r.text
    pdf_urls = _PDF_LINK_RE.findall(html)
    result: dict[str, str] = {}
    for url in pdf_urls:
        full_url = url if url.startswith("http") else INFO_NEC_BASE + url
        url_lower = url.lower()
        for key, keywords in _KEYWORD_MAP.items():
            if key in result:
                continue
            if any(k in url_lower for k in keywords):
                result[key] = full_url
                break
    return result
