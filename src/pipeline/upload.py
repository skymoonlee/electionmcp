"""
Parquet → HuggingFace Dataset 업로드.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import typer
from datasets import Dataset, DatasetDict
from dotenv import load_dotenv
from huggingface_hub import HfApi
from rich.console import Console

load_dotenv()
console = Console()
app = typer.Typer()


# 시도명 → 영문 슬러그 (HF split 이름용)
SIDO_SLUG = {
    "서울특별시": "seoul",
    "부산광역시": "busan",
    "대구광역시": "daegu",
    "인천광역시": "incheon",
    "광주광역시": "gwangju",
    "대전광역시": "daejeon",
    "울산광역시": "ulsan",
    "세종특별자치시": "sejong",
    "경기도": "gyeonggi",
    "강원특별자치도": "gangwon",
    "강원도": "gangwon",
    "충청북도": "chungbuk",
    "충청남도": "chungnam",
    "전북특별자치도": "jeonbuk",
    "전라북도": "jeonbuk",
    "전라남도": "jeonnam",
    "전남광주통합특별시": "honam",  # 9회 지선부터 광주+전남 통합
    "경상북도": "gyeongbuk",
    "경상남도": "gyeongnam",
    "제주특별자치도": "jeju",
}


DATASET_CARD = """\
---
language:
- ko
license: other
license_name: kogl-type-1
license_link: https://www.kogl.or.kr/info/license.do#01-tab
tags:
- elections
- korea
- politics
- nec
- mcp
size_categories:
- 1K<n<10K
---

# 9회 전국동시지방선거 (2026.6.3) 후보자 데이터셋

대한민국 제9회 전국동시지방선거 후보자 정보를 중앙선거관리위원회 공식 데이터에서
수집·정제한 데이터셋. AI 자연어 인터페이스(MCP) 와 결합되어 유권자가 자기 지역구
후보 정보를 손쉽게 조회·비교할 수 있도록 제공된다.

## 관련 프로젝트

| 항목 | 링크 |
|---|---|
| GitHub 저장소 (코드) | https://github.com/skymoonlee/electionmcp |
| MCP 서버 (라이브) | https://mcp.electionmcp.kr/mcp |
| 헬스체크 | https://mcp.electionmcp.kr/health |

본 데이터셋만으로도 분석·연구에 사용 가능하지만, MCP 서버를 통한
Claude·Cursor·Cline 등 AI 클라이언트 연동이 권장된다. 자세한 사용법은
GitHub 저장소의 README 참조.

## 출처
- 중앙선거관리위원회 공공데이터포털 OpenAPI (공공누리 1유형)
  - 후보자 정보, 선거공약 정보, 코드 정보
- 중앙선거관리위원회 정보공개자료 (info.nec.go.kr)
  - 전과기록, 재산신고, 병역, 학력, 납세

## 라이선스
공공누리 제1유형 (출처표시) — 출처: **중앙선거관리위원회**

## 사용 목적
공직선거법 제49조에 따른 **국민의 알권리 및 선거권 행사 보장** 목적의 정보 제공.

## 스키마
- `hubo_id`: 후보자 ID
- `sg_type` / `sg_type_name`: 선거 종류 코드 / 명칭
- `sido`, `sgg`, `district`: 광역시도, 시군구, 선거구
- `name`, `party`, `ballot_number`: 이름, 정당, 기호
- `stage`: preliminary | official
- `pledges`: 공약 리스트
- `criminal`, `assets`, `military`, `education`, `tax`: 정보공개자료 텍스트
- `extraction_methods`: PDF 추출 방법 (pdfplumber/ocr/failed)
- `source_urls`: 원본 URL

## 주의사항
선거 결과의 평가나 후보 추천에 이 데이터를 사용하지 마십시오.
사실 정보 조회 및 비교 목적으로만 사용하십시오.
"""


@app.command()
def push(
    parquet_dir: str = typer.Option("./data/parquet"),
    repo_id: str = typer.Option(None, envvar="HF_DATASET_REPO"),
    hf_token: str = typer.Option(None, envvar="HF_TOKEN"),
    private: bool = typer.Option(False),
):
    """Parquet → HuggingFace Hub 업로드."""
    repo_id = repo_id or os.getenv("HF_DATASET_REPO")
    hf_token = hf_token or os.getenv("HF_TOKEN")
    if not repo_id or not hf_token or hf_token == "hf_REPLACE_ME":
        console.print("[red]HF_DATASET_REPO 와 유효한 HF_TOKEN 환경변수 필요[/red]")
        raise typer.Exit(1)

    all_path = Path(parquet_dir) / "all.parquet"
    if not all_path.exists():
        console.print(f"[red]{all_path} 가 없습니다. pipeline.ingest 먼저 실행하세요.[/red]")
        raise typer.Exit(1)

    console.print("[cyan]Parquet 로드 중...[/cyan]")
    df = pd.read_parquet(all_path)
    console.print(f"  총 {len(df)}건")

    # 시도별 split
    splits: dict[str, Dataset] = {}
    for sido, group in df.groupby("sido"):
        if not sido:
            continue
        slug = SIDO_SLUG.get(sido)
        if slug is None:
            console.print(f"[yellow]알 수 없는 시도명: {sido} — split 'unknown' 으로 들어감[/yellow]")
            slug = "unknown"
        splits[slug] = Dataset.from_pandas(group, preserve_index=False)
    splits["all"] = Dataset.from_pandas(df, preserve_index=False)
    dataset = DatasetDict(splits)

    console.print(f"[cyan]Pushing to {repo_id}...[/cyan]")
    dataset.push_to_hub(
        repo_id,
        token=hf_token,
        private=private,
        commit_message="9회 전국동시지방선거 후보자 데이터 업데이트",
    )

    api = HfApi(token=hf_token)
    readme_path = Path("./_dataset_README.md")
    readme_path.write_text(DATASET_CARD, encoding="utf-8")
    api.upload_file(
        path_or_fileobj=str(readme_path),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )
    console.print(f"[green]✓ {repo_id} 업로드 완료[/green]")


def main():
    app()


if __name__ == "__main__":
    main()
