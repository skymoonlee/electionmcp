"""
전체 수집 파이프라인.

단계:
1. 후보자 인덱스 수집 (17시도 × 7선거종류)
2. 공약 수집 (sg_type 3/4/11)
3. info.nec.go.kr 정보공개자료 PDF 수집 + OCR
4. Parquet 저장 (시도별 파티셔닝)

체크포인트: 단계별 SQLite에 진행상태 기록 → 중단 후 resume 가능.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from nec.client import NECClient, SG_TYPES_LOCAL_ELECTION
from pdf.processor import PDFProcessor, discover_pdf_urls

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pipeline")
console = Console()
app = typer.Typer()


class Checkpoint:
    """SQLite 기반 진행상태 추적."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS candidates (
            hubo_id TEXT PRIMARY KEY,
            sg_id TEXT,
            sg_type TEXT,
            sg_type_name TEXT,
            sido TEXT,
            sgg TEXT,
            district TEXT,
            name TEXT,
            party TEXT,
            ballot_number INTEGER,
            stage TEXT,
            raw_json TEXT,
            collected_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pledges (
            hubo_id TEXT,
            pledge_no INTEGER,
            title TEXT,
            content TEXT,
            collected_at TEXT,
            PRIMARY KEY (hubo_id, pledge_no)
        );
        CREATE TABLE IF NOT EXISTS disclosures (
            hubo_id TEXT PRIMARY KEY,
            criminal TEXT,
            assets TEXT,
            military TEXT,
            education TEXT,
            tax TEXT,
            extraction_methods TEXT,
            source_urls TEXT,
            collected_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cand_sgg ON candidates(sido, sgg);
        CREATE INDEX IF NOT EXISTS idx_cand_district ON candidates(district);
        CREATE INDEX IF NOT EXISTS idx_cand_sgtype ON candidates(sg_type);
        """)

    def upsert_candidate(self, c: dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                c["hubo_id"], c["sg_id"], c["sg_type"], c.get("sg_type_name"),
                c.get("sido"), c.get("sgg"), c.get("district"),
                c["name"], c.get("party"), c.get("ballot_number"),
                c.get("stage"),
                json.dumps(c["raw"], ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()

    def replace_pledges(self, hubo_id: str, pledges: list[dict]):
        """기존 공약 삭제 후 새로 입력 (stale row 방지)."""
        self.conn.execute("DELETE FROM pledges WHERE hubo_id = ?", [hubo_id])
        for i, p in enumerate(pledges):
            self.conn.execute(
                "INSERT INTO pledges VALUES (?,?,?,?,?)",
                (
                    hubo_id, i,
                    p.get("prmsTitle") or p.get("title"),
                    p.get("prmsCn") or p.get("content"),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        self.conn.commit()

    def upsert_disclosure(self, doc):
        self.conn.execute(
            "INSERT OR REPLACE INTO disclosures VALUES (?,?,?,?,?,?,?,?,?)",
            (
                doc.hubo_id,
                doc.documents.get("criminal", ""),
                doc.documents.get("assets", ""),
                doc.documents.get("military", ""),
                doc.documents.get("education", ""),
                doc.documents.get("tax", ""),
                json.dumps(doc.extraction_method, ensure_ascii=False),
                json.dumps(doc.source_urls, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()

    def all_hubo_ids(self) -> list[tuple[str, str]]:
        cur = self.conn.execute("SELECT hubo_id, sg_type FROM candidates")
        return cur.fetchall()

    def disclosed_hubo_ids(self) -> set[str]:
        cur = self.conn.execute("SELECT hubo_id FROM disclosures")
        return {row[0] for row in cur.fetchall()}


def normalize_candidate(raw: dict, sg_id: str, sg_type: str, stage: str) -> dict:
    """NEC API 응답 → 정규화된 dict.

    NEC 응답은 필드명이 엔드포인트마다 약간 달라서 여러 키를 시도한다.
    """
    hubo_id = (
        raw.get("huboid")
        or raw.get("huboId")
        or raw.get("cnddtId")
        or raw.get("CnddtId")
    )
    giho = raw.get("giho") or raw.get("gihoSn") or ""
    ballot_number = int(giho) if str(giho).isdigit() else None

    return {
        "hubo_id": hubo_id,
        "sg_id": sg_id,
        "sg_type": sg_type,
        "sg_type_name": SG_TYPES_LOCAL_ELECTION.get(sg_type, sg_type),
        "sido": raw.get("sdName") or raw.get("sdNm"),
        "sgg": raw.get("sggName") or raw.get("wiwName") or raw.get("gusiguName"),
        "district": (
            raw.get("giho_jibang")
            or raw.get("sggName")
            or raw.get("sgGuName")
        ),
        "name": raw.get("name") or raw.get("krName") or raw.get("hanglName"),
        "party": raw.get("jdName") or raw.get("partyName"),
        "ballot_number": ballot_number,
        "stage": stage,
        "raw": raw,
    }


# ---------- Stage 1: 후보자 인덱스 ----------

async def stage1_candidates(
    nec: NECClient,
    sg_id: str,
    ckpt: Checkpoint,
    stage: str,
):
    console.print(f"[bold cyan]Stage 1: 후보자 인덱스 수집 ({stage})[/bold cyan]")
    with Progress(
        SpinnerColumn(), TextColumn("{task.description}"),
        BarColumn(), TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(), console=console,
    ) as prog:
        items = list(SG_TYPES_LOCAL_ELECTION.items())
        task_id = prog.add_task("선거종류별 수집", total=len(items))
        total_candidates = 0
        for sg_type, name in items:
            prog.update(task_id, description=f"{name} 수집 중")
            count = 0
            async for raw in nec.iter_candidates(sg_id, sg_type, stage=stage):
                cand = normalize_candidate(raw, sg_id, sg_type, stage)
                if cand["hubo_id"]:
                    ckpt.upsert_candidate(cand)
                    count += 1
            console.print(f"  {name}: {count}명")
            total_candidates += count
            prog.advance(task_id)
        console.print(f"[green]총 {total_candidates}명 수집[/green]")


# ---------- Stage 2: 공약 ----------

async def stage2_pledges(nec: NECClient, sg_id: str, ckpt: Checkpoint):
    console.print("[bold cyan]Stage 2: 공약 수집[/bold cyan]")
    candidates = ckpt.all_hubo_ids()
    targets = [(h, t) for h, t in candidates if t in ("3", "4", "11")]
    console.print(f"  공약 대상: {len(targets)}명 (시도지사+구시군장+교육감)")

    with Progress(
        SpinnerColumn(), TextColumn("[bold]공약 수집"),
        BarColumn(), TextColumn("{task.completed}/{task.total}"),
        TimeRemainingColumn(), console=console,
    ) as prog:
        task_id = prog.add_task("", total=len(targets))
        for hubo_id, sg_type in targets:
            pledges = await nec.get_pledges(sg_id, sg_type, hubo_id)
            ckpt.replace_pledges(hubo_id, pledges)
            prog.advance(task_id)


# ---------- Stage 3: 정보공개자료 PDF ----------

async def stage3_disclosures(
    sg_id: str, ckpt: Checkpoint, pdf_dir: str, concurrency: int
):
    console.print("[bold cyan]Stage 3: 정보공개자료 PDF + OCR[/bold cyan]")
    candidates = ckpt.all_hubo_ids()
    done = ckpt.disclosed_hubo_ids()
    todo = [h for h, _ in candidates if h not in done]
    console.print(f"  처리 대상: {len(todo)}명 (이미 완료: {len(done)}명)")

    async with PDFProcessor(pdf_dir, concurrency=concurrency) as pdf_proc:
        with Progress(
            SpinnerColumn(), TextColumn("[bold]PDF + OCR"),
            BarColumn(), TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(), console=console,
        ) as prog:
            task_id = prog.add_task("", total=len(todo))
            for hubo_id in todo:
                try:
                    urls = await discover_pdf_urls(pdf_proc.client, sg_id, hubo_id)
                except Exception as e:
                    log.warning("URL discovery 실패 (hubo=%s): %s", hubo_id, e)
                    urls = {}
                if not urls:
                    prog.advance(task_id)
                    continue
                doc = await pdf_proc.process_candidate(hubo_id, sg_id, urls)
                ckpt.upsert_disclosure(doc)
                prog.advance(task_id)


# ---------- Stage 4: Parquet export ----------

def stage4_export_parquet(ckpt_path: str, parquet_dir: str):
    console.print("[bold cyan]Stage 4: Parquet 변환[/bold cyan]")
    Path(parquet_dir).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(ckpt_path)

    cands = pd.read_sql_query("SELECT * FROM candidates", conn)
    pledges = pd.read_sql_query("SELECT * FROM pledges", conn)
    disclosures = pd.read_sql_query("SELECT * FROM disclosures", conn)

    if cands.empty:
        console.print("[red]candidates 테이블이 비어있음. stage 1 먼저 실행.[/red]")
        return

    # 후보자 + 공약 join
    if not pledges.empty:
        pledges_grouped = (
            pledges.sort_values(["hubo_id", "pledge_no"])
                   .groupby("hubo_id")[["pledge_no", "title", "content"]]
                   .apply(lambda g: g.to_dict("records"))
                   .to_frame("pledges").reset_index()
        )
        merged = cands.merge(pledges_grouped, on="hubo_id", how="left")
    else:
        merged = cands.copy()
        merged["pledges"] = None

    if not disclosures.empty:
        merged = merged.merge(disclosures, on="hubo_id", how="left")

    # 시도별 파티셔닝
    for sido, group in merged.groupby("sido"):
        if not sido:
            continue
        out = Path(parquet_dir) / f"sido={sido}"
        out.mkdir(parents=True, exist_ok=True)
        group.to_parquet(out / "data.parquet", index=False)

    merged.to_parquet(Path(parquet_dir) / "all.parquet", index=False)
    console.print(f"[green]총 {len(merged)}건 → {parquet_dir}[/green]")


# ---------- 통합 async 진입점 ----------

async def run_async_stages(
    api_key: str,
    sg_id: str,
    ckpt: Checkpoint,
    rps: int,
    pdf_dir: str,
    concurrency: int,
    stage: str,
    skip_pdfs: bool,
):
    """단일 이벤트 루프에서 stage 1-3 실행."""
    async with NECClient(api_key, rps=rps) as nec:
        await stage1_candidates(nec, sg_id, ckpt, stage)
        await stage2_pledges(nec, sg_id, ckpt)
    if not skip_pdfs:
        await stage3_disclosures(sg_id, ckpt, pdf_dir, concurrency)


# ---------- CLI ----------

def _resolve_env(value: Optional[str], key: str, default: Optional[str] = None) -> Optional[str]:
    return value or os.getenv(key, default)


@app.command(name="all")
def run_all(
    sg_id: Optional[str] = typer.Option(None, envvar="SG_ID"),
    api_key: Optional[str] = typer.Option(None, envvar="NEC_API_KEY"),
    data_dir: str = typer.Option("./data", envvar="DATA_DIR"),
    rps: int = typer.Option(5, envvar="NEC_RPS"),
    concurrency: int = typer.Option(10, envvar="PDF_CONCURRENCY"),
    stage: str = typer.Option("official", envvar="CANDIDATE_STAGE",
                              help="preliminary|official"),
    skip_pdfs: bool = typer.Option(False, help="PDF/OCR 단계 스킵 (디버그용)"),
):
    """전 단계 실행."""
    sg_id = _resolve_env(sg_id, "SG_ID", "20260603")
    api_key = _resolve_env(api_key, "NEC_API_KEY")
    if not api_key:
        console.print("[red]NEC_API_KEY 환경변수가 없습니다[/red]")
        raise typer.Exit(1)

    ckpt_path = Path(data_dir) / "checkpoint.db"
    pdf_dir = str(Path(data_dir) / "pdfs")
    parquet_dir = str(Path(data_dir) / "parquet")
    ckpt = Checkpoint(ckpt_path)

    # 단일 asyncio.run() 으로 stage 1-3 실행
    asyncio.run(run_async_stages(
        api_key, sg_id, ckpt, rps, pdf_dir, concurrency, stage, skip_pdfs
    ))
    # stage 4 는 동기
    stage4_export_parquet(str(ckpt_path), parquet_dir)


@app.command()
def stage(
    n: int = typer.Argument(..., help="실행할 단계 (1-4)"),
    sg_id: Optional[str] = typer.Option(None, envvar="SG_ID"),
    api_key: Optional[str] = typer.Option(None, envvar="NEC_API_KEY"),
    data_dir: str = typer.Option("./data", envvar="DATA_DIR"),
    rps: int = typer.Option(5, envvar="NEC_RPS"),
    concurrency: int = typer.Option(10, envvar="PDF_CONCURRENCY"),
    candidate_stage: str = typer.Option("official", envvar="CANDIDATE_STAGE"),
):
    """특정 단계만 실행."""
    sg_id = _resolve_env(sg_id, "SG_ID", "20260603")
    api_key = _resolve_env(api_key, "NEC_API_KEY")
    ckpt_path = Path(data_dir) / "checkpoint.db"
    ckpt = Checkpoint(ckpt_path)

    async def _run_one():
        async with NECClient(api_key, rps=rps) as nec:
            if n == 1:
                await stage1_candidates(nec, sg_id, ckpt, candidate_stage)
            elif n == 2:
                await stage2_pledges(nec, sg_id, ckpt)
            elif n == 3:
                await stage3_disclosures(
                    sg_id, ckpt, str(Path(data_dir) / "pdfs"), concurrency,
                )

    if n in (1, 2, 3):
        if not api_key and n in (1, 2):
            console.print("[red]NEC_API_KEY 환경변수가 없습니다[/red]")
            raise typer.Exit(1)
        asyncio.run(_run_one())
    elif n == 4:
        stage4_export_parquet(str(ckpt_path), str(Path(data_dir) / "parquet"))
    else:
        console.print(f"[red]잘못된 단계: {n}[/red]")
        raise typer.Exit(1)


@app.command()
def verify_sgid(
    api_key: Optional[str] = typer.Option(None, envvar="NEC_API_KEY"),
):
    """9회 지선 sgId 확정 — CommonCodeService 로 선거 코드 목록 조회."""
    api_key = _resolve_env(api_key, "NEC_API_KEY")
    if not api_key:
        console.print("[red]NEC_API_KEY 환경변수가 없습니다[/red]")
        raise typer.Exit(1)

    async def _run():
        async with NECClient(api_key) as nec:
            codes = await nec.get_election_codes()
            for c in codes:
                console.print(c)

    asyncio.run(_run())


def main():
    app()


if __name__ == "__main__":
    main()
