#!/usr/bin/env python3
"""
Feed Generator

Reads YAML filter configs from feeds/ and generates filtered output files
(JSON, CSV, RSS) from the SQLite canonical database.

Usage:
    python feed_generator.py                    # generate all feeds
    python feed_generator.py --feed nursing_jobs_all   # one specific feed
    python feed_generator.py --dry-run          # show SQL + counts, no writes
    python feed_generator.py --list             # list available feed configs
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

import yaml

from config.settings import DB_PATH, FEEDS_DIR, FEEDS_OUTPUT_DIR
from storage.database import init_db, get_connection, query_jobs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Config loading
# ──────────────────────────────────────────────

def load_feed_configs(feeds_dir: Path, feed_name: str | None = None) -> list[dict]:
    """Load YAML feed configs from the feeds directory."""
    configs = []
    if not feeds_dir.is_dir():
        logger.error(f"Feeds directory not found: {feeds_dir}")
        return configs

    for path in sorted(feeds_dir.glob("*.yaml")):
        if feed_name and path.stem != feed_name:
            continue
        try:
            with open(path) as f:
                cfg = yaml.safe_load(f)
            cfg["_file"] = str(path)
            cfg["_stem"] = path.stem
            configs.append(cfg)
        except Exception as e:
            logger.warning(f"Failed to load {path}: {e}")

    if feed_name and not configs:
        logger.error(f"Feed config '{feed_name}' not found in {feeds_dir}")

    return configs


# ──────────────────────────────────────────────
# Output writers
# ──────────────────────────────────────────────

def row_to_dict(row) -> dict:
    """Convert a sqlite3.Row to a plain dict suitable for JSON/CSV."""
    d = dict(row)
    if d.get("categories"):
        try:
            d["categories"] = json.loads(d["categories"])
        except (json.JSONDecodeError, TypeError):
            d["categories"] = []
    d["is_nursing"] = bool(d.get("is_nursing"))
    return d


def write_json(rows: list, output_dir: Path, feed_name: str) -> Path:
    """Write filtered jobs to JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "jobs.json"
    data = {
        "feed": feed_name,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "count": len(rows),
        "jobs": [row_to_dict(r) for r in rows],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    return path


def write_csv(rows: list, output_dir: Path) -> Path:
    """Write filtered jobs to CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "jobs.csv"

    if not rows:
        path.write_text("")
        return path

    first = row_to_dict(rows[0])
    fieldnames = list(first.keys())

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            d = row_to_dict(row)
            if isinstance(d.get("categories"), list):
                d["categories"] = "; ".join(str(c) for c in d["categories"])
            writer.writerow(d)

    return path


def write_rss(rows: list, output_dir: Path, feed_name: str, description: str = "") -> Path:
    """Write filtered jobs to RSS 2.0 XML."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "feed.xml"

    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = feed_name
    SubElement(channel, "description").text = description or feed_name
    SubElement(channel, "lastBuildDate").text = datetime.utcnow().strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    for row in rows[:500]:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = row["title"]
        SubElement(item, "link").text = row["url"] or ""
        desc_parts = []
        if row["company_name"]:
            desc_parts.append(row["company_name"])
        if row["location"]:
            desc_parts.append(row["location"])
        if row["job_type"]:
            desc_parts.append(row["job_type"])
        SubElement(item, "description").text = " | ".join(desc_parts)
        if row["posted_date"]:
            SubElement(item, "pubDate").text = str(row["posted_date"])

    xml_bytes = tostring(rss, encoding="unicode", xml_declaration=True)
    path.write_text(xml_bytes, encoding="utf-8")
    return path


# ──────────────────────────────────────────────
# Feed generation
# ──────────────────────────────────────────────

def generate_feed(config: dict, conn, dry_run: bool = False) -> dict:
    """Generate output files for a single feed config. Returns stats dict."""
    name = config.get("name", config["_stem"])
    filters = config.get("filters", {})
    output_formats = config.get("output_formats", ["json"])
    output_dir_str = config.get("output_dir", f"data/feeds/{config['_stem']}")
    output_dir = Path(output_dir_str)
    if not output_dir.is_absolute():
        from config.settings import PROJECT_ROOT
        output_dir = PROJECT_ROOT / output_dir

    logger.info(f"Generating feed: {name}")
    logger.info(f"  Filters: {filters}")

    rows = query_jobs(
        conn,
        sectors=filters.get("sectors"),
        states=filters.get("states"),
        is_nursing=filters.get("is_nursing"),
        categories=filters.get("categories"),
        title_keywords=filters.get("title_keywords"),
        exclude_keywords=filters.get("exclude_keywords"),
        posted_within_days=filters.get("posted_within_days"),
        salary_min=filters.get("salary_min"),
        ats_types=filters.get("ats_types"),
        limit=filters.get("limit"),
    )

    logger.info(f"  Matched {len(rows)} jobs")

    if dry_run:
        logger.info("  [dry-run] Skipping file writes")
        return {"name": name, "jobs": len(rows), "files": []}

    files_written = []
    for fmt in output_formats:
        fmt = fmt.lower()
        if fmt == "json":
            p = write_json(rows, output_dir, name)
            files_written.append(str(p))
            logger.info(f"  Wrote {p}")
        elif fmt == "csv":
            p = write_csv(rows, output_dir)
            files_written.append(str(p))
            logger.info(f"  Wrote {p}")
        elif fmt in ("rss", "xml"):
            p = write_rss(rows, output_dir, name, config.get("description", ""))
            files_written.append(str(p))
            logger.info(f"  Wrote {p}")
        else:
            logger.warning(f"  Unknown output format: {fmt}")

    return {"name": name, "jobs": len(rows), "files": files_written}


def generate_all_feeds(
    feeds_dir: Path = FEEDS_DIR,
    db_path: Path = DB_PATH,
    feed_name: str | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Generate all (or one specific) feed. Returns list of stats dicts."""
    configs = load_feed_configs(feeds_dir, feed_name)
    if not configs:
        logger.warning("No feed configs found")
        return []

    init_db(db_path)
    conn = get_connection(db_path)

    results = []
    try:
        for cfg in configs:
            stats = generate_feed(cfg, conn, dry_run=dry_run)
            results.append(stats)
    finally:
        conn.close()

    return results


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate filtered job feeds")
    parser.add_argument("--feed", default=None, help="Generate a specific feed by name (YAML stem)")
    parser.add_argument("--dry-run", action="store_true", help="Show query results without writing files")
    parser.add_argument("--list", action="store_true", help="List available feed configs")
    parser.add_argument("--feeds-dir", default=str(FEEDS_DIR), help="Directory containing feed YAML configs")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to SQLite database")
    args = parser.parse_args()

    feeds_dir = Path(args.feeds_dir)
    db_path = Path(args.db)

    if args.list:
        configs = load_feed_configs(feeds_dir)
        if not configs:
            print("No feed configs found")
            return
        print(f"\nAvailable feeds ({len(configs)}):\n")
        for cfg in configs:
            filters = cfg.get("filters", {})
            formats = ", ".join(cfg.get("output_formats", ["json"]))
            print(f"  {cfg['_stem']:<30} {cfg.get('name', '')}")
            print(f"    {'Formats:':<12} {formats}")
            print(f"    {'Filters:':<12} {filters}")
            print()
        return

    start = time.time()
    results = generate_all_feeds(
        feeds_dir=feeds_dir,
        db_path=db_path,
        feed_name=args.feed,
        dry_run=args.dry_run,
    )

    elapsed = time.time() - start
    total_jobs = sum(r["jobs"] for r in results)
    total_files = sum(len(r["files"]) for r in results)

    print(f"\n{'=' * 60}")
    print(f"Feed generation complete in {elapsed:.1f}s")
    print(f"  Feeds processed: {len(results)}")
    print(f"  Total jobs matched: {total_jobs}")
    print(f"  Files written: {total_files}")
    print(f"{'=' * 60}")

    for r in results:
        print(f"\n  {r['name']}: {r['jobs']} jobs")
        for f in r["files"]:
            print(f"    -> {f}")


if __name__ == "__main__":
    main()
