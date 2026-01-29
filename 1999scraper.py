from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, Optional

BASE_1999 = "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/1999"


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _cell_text(cell) -> str:
    return _norm_space(cell.get_text(" ", strip=True))


def _safe_psv_field(s: str) -> str:
    return (s or "").replace("|", " ").strip()


@dataclass(frozen=True)
class Row:
    report_date: date
    unit: str
    power: str
    reason: str

    def to_psv(self) -> str:
        d = f"{self.report_date.month}/{self.report_date.day}/{self.report_date.year}"
        return (
            f"{d}|{_safe_psv_field(self.unit)}|{_safe_psv_field(self.power)}|{_safe_psv_field(self.reason)}"
        )


def iter_dates_1999() -> Iterable[date]:
    d = date(1999, 1, 1)
    end = date(1999, 12, 31)
    while d <= end:
        yield d
        d += timedelta(days=1)


def url_for_day(d: date) -> str:
    return f"{BASE_1999}/{d.strftime('%Y%m%d')}ps.html"


class _TableExtractor(HTMLParser):

    def __init__(self) -> None:
        super().__init__()
        self._in_table = 0
        self._in_tr = False
        self._in_cell = False
        self._cell_buf: list[str] = []
        self._current_row: list[str] = []
        self._current_table: list[list[str]] = []
        self.tables: list[list[list[str]]] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "table":
            self._in_table += 1
            if self._in_table == 1:
                self._current_table = []
        if self._in_table >= 1 and tag == "tr":
            self._in_tr = True
            self._current_row = []
        if self._in_table >= 1 and self._in_tr and tag in {"td", "th"}:
            self._in_cell = True
            self._cell_buf = []
        if self._in_table >= 1 and self._in_cell and tag in {"br", "p"}:
            self._cell_buf.append(" ")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self._in_table >= 1 and self._in_tr and tag in {"td", "th"} and self._in_cell:
            self._in_cell = False
            cell_text = _norm_space("".join(self._cell_buf))
            self._current_row.append(cell_text)
            self._cell_buf = []
        if self._in_table >= 1 and tag == "tr" and self._in_tr:
            self._in_tr = False
            if any(c.strip() for c in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = []
        if tag == "table" and self._in_table >= 1:
            self._in_table -= 1
            if self._in_table == 0:
                if self._current_table:
                    self.tables.append(self._current_table)
                self._current_table = []

    def handle_data(self, data: str) -> None:
        if self._in_table >= 1 and self._in_cell:
            self._cell_buf.append(data)


def fetch_html(
    url: str,
    cache_path: Optional[Path],
    retries: int = 5,
    backoff_s: float = 1.25,
) -> str:
    if cache_path is not None and cache_path.exists():
        return cache_path.read_text(encoding="utf-8", errors="replace")

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            proc = subprocess.run(
                ["curl", "-fsSL", url],
                check=False,
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                raise RuntimeError(_norm_space(proc.stderr) or f"curl exit {proc.returncode}")
            html = proc.stdout
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(html, encoding="utf-8")
            return html
        except BaseException as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s * attempt)
                continue
            break

    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def _find_col_idx(headers: list[str], pred) -> Optional[int]:
    for i, h in enumerate(headers):
        if pred(h):
            return i
    return None


def parse_rows(report_date: date, html: str) -> list[Row]:
    out: list[Row] = []

    parser = _TableExtractor()
    parser.feed(html)
    tables = parser.tables

    for table in tables:
        header_row_idx: Optional[int] = None
        headers: list[str] = []
        for ridx, row in enumerate(table):
            lowered = [_norm_space(c).lower() for c in row]
            if any(c == "unit" for c in lowered) and any(c.startswith("power") or c == "power" for c in lowered):
                header_row_idx = ridx
                headers = lowered
                break
        if header_row_idx is None or not headers:
            continue

        unit_idx = _find_col_idx(headers, lambda h: h == "unit" or h.startswith("unit "))
        power_idx = _find_col_idx(headers, lambda h: h == "power" or h.startswith("power "))
        reason_idx = _find_col_idx(headers, lambda h: "reason" in h and "comment" in h)

        if unit_idx is None or power_idx is None:
            continue

        for row in table[header_row_idx + 1 :]:
            if unit_idx >= len(row) or power_idx >= len(row):
                continue
            unit = _norm_space(row[unit_idx])
            power = _norm_space(row[power_idx])
            reason = _norm_space(row[reason_idx]) if (reason_idx is not None and reason_idx < len(row)) else ""

            if not unit or unit.lower() == "unit":
                continue

            out.append(Row(report_date=report_date, unit=unit, power=power, reason=reason))

    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Scrape NRC 1999 daily power reactor status reports.")
    ap.add_argument(
        "--out",
        default=str(Path("output") / "nrc_reactor_status_1999.psv"),
        help="Output pipe-delimited file path.",
    )
    ap.add_argument(
        "--cache-dir",
        default=str(Path(".cache") / "nrc_1999_html"),
        help="Directory to cache fetched HTML (speeds up reruns).",
    )
    ap.add_argument("--no-cache", action="store_true", help="Disable caching HTML to disk.")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cache_dir = None if args.no_cache else Path(args.cache_dir)
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    with out_path.open("w", encoding="utf-8", newline="\n") as f:
        for i, d in enumerate(iter_dates_1999(), start=1):
            url = url_for_day(d)
            cache_path = (cache_dir / f"{d.strftime('%Y%m%d')}.html") if cache_dir is not None else None
            try:
                html = fetch_html(url, cache_path=cache_path)
                rows = parse_rows(d, html)
                for r in rows:
                    f.write(r.to_psv() + "\n")
                total_rows += len(rows)
            except Exception as e:
                print(f"[WARN] {d.isoformat()} failed: {e}", file=sys.stderr)

            if i % 10 == 0:
                print(f"[INFO] processed {i}/365 days; rows so far: {total_rows}", file=sys.stderr)

    print(f"[DONE] wrote {total_rows} rows to {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
