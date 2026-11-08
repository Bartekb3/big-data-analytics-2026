"""Summary of the raw archive per feed and day: calls, outcomes, records/s, size (input for the M2 data description)."""

import gzip
import json
from collections import defaultdict
from pathlib import Path


def summarize_records(records) -> dict:
    """Counts for an iterable of raw records."""
    out = {"calls": 0, "ok": 0, "http_429": 0, "other_status": 0, "errors": 0, "first": None, "last": None}
    for rec in records:
        out["calls"] += 1
        status = rec.get("status")
        if status is None:
            out["errors"] += 1
        elif status == 200:
            out["ok"] += 1
        elif status == 429:
            out["http_429"] += 1
        else:
            out["other_status"] += 1
        ts = rec["requested_at"]
        out["first"] = ts if out["first"] is None or ts < out["first"] else out["first"]
        out["last"] = ts if out["last"] is None or ts > out["last"] else out["last"]
    return out


def read_lines(path: Path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except ValueError:
                    continue  # partial last line after a crash


def archive_summary(raw_dir: Path) -> list[dict]:
    """One row per (feed, date)."""
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for p in raw_dir.glob("*/date=*/*.jsonl*"):
        if p.name.endswith(".tmp"):
            continue
        groups[(p.parent.parent.name, p.parent.name.removeprefix("date="))].append(p)
    rows = []
    for (feed, day), paths in sorted(groups.items()):
        summary = summarize_records(r for p in sorted(paths) for r in read_lines(p))
        rows.append({"feed": feed, "date": day, **summary, "mb_on_disk": round(sum(p.stat().st_size for p in paths) / 1e6, 2)})
    return rows
