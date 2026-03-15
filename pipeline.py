#!/usr/bin/env python3
"""
Daily Pipeline Orchestrator

Single entry point for the daily cron job:
  1. Discover new portals (subdomain enumeration + probing)
  2. Scrape jobs from all active portals
  3. Generate filtered feeds from the SQLite database

Usage:
    python pipeline.py daily                # full pipeline
    python pipeline.py daily --skip-discovery   # scrape + feeds only
    python pipeline.py daily --skip-scrape      # discover + feeds only
    python pipeline.py daily --feeds-only       # regenerate feeds only
    python pipeline.py status               # show last run stats
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from config.settings import DB_PATH, FEEDS_DIR, PROJECT_ROOT
from storage.database import init_db, db_session, start_run, finish_run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_step(description: str, cmd: list[str]) -> tuple[bool, str]:
    """Run a subprocess step. Returns (success, output)."""
    logger.info(f"{'=' * 60}")
    logger.info(f"PIPELINE: {description}")
    logger.info(f"  Command: {' '.join(cmd)}")
    logger.info(f"{'=' * 60}")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=False,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            logger.error(f"Step failed with exit code {result.returncode}")
            return False, f"Exit code {result.returncode}"
        return True, ""
    except subprocess.TimeoutExpired:
        logger.error(f"Step timed out after 1 hour")
        return False, "Timeout after 3600s"
    except Exception as e:
        logger.error(f"Step failed: {e}")
        return False, str(e)


def run_daily(
    skip_discovery: bool = False,
    skip_scrape: bool = False,
    feeds_only: bool = False,
    discovery_workers: int = 50,
    scrape_limit: int | None = None,
    scrape_portal: str | None = None,
    today_only: bool = True,
):
    """Execute the full daily pipeline."""
    python = sys.executable
    start = time.time()

    init_db(DB_PATH)

    with db_session(DB_PATH) as conn:
        run_id = start_run(conn, "daily")

    portals_found = 0
    jobs_found = 0
    feeds_generated = 0
    errors = []

    # Step 1: Discovery
    if not feeds_only and not skip_discovery:
        cmd = [
            python, "discover_all.py",
            "--workers", str(discovery_workers),
        ]
        ok, err = run_step("Discover new iCIMS portals", cmd)
        if not ok:
            errors.append(f"Discovery: {err}")
        else:
            with db_session(DB_PATH) as conn:
                row = conn.execute("SELECT COUNT(*) as cnt FROM portals").fetchone()
                portals_found = row["cnt"]
    else:
        logger.info("Skipping discovery step")

    # Step 2a: Scrape iCIMS portals
    if not feeds_only and not skip_scrape:
        cmd = [python, "main.py", "scrape", "--ats", "icims", "--from-db", "--sector", "healthcare"]
        if scrape_limit:
            cmd.extend(["--limit", str(scrape_limit)])
        if scrape_portal:
            cmd.extend(["--portal", scrape_portal])
        if today_only:
            cmd.extend(["--today-only"])

        ok, err = run_step("Scrape iCIMS portals", cmd)
        if not ok:
            errors.append(f"iCIMS Scrape: {err}")

    # Step 2b: Scrape Workday portals
    if not feeds_only and not skip_scrape:
        cmd = [python, "main.py", "scrape", "--ats", "workday", "--from-db"]
        if scrape_limit:
            cmd.extend(["--limit", str(scrape_limit)])
        if today_only:
            cmd.extend(["--today-only"])

        ok, err = run_step("Scrape Workday portals", cmd)
        if not ok:
            errors.append(f"Workday Scrape: {err}")

    # Step 2c: Scrape TalentBrew portals
    if not feeds_only and not skip_scrape:
        cmd = [python, "main.py", "scrape", "--ats", "talentbrew", "--from-db"]
        if scrape_limit:
            cmd.extend(["--limit", str(scrape_limit)])
        if today_only:
            cmd.extend(["--today-only"])

        ok, err = run_step("Scrape TalentBrew portals", cmd)
        if not ok:
            errors.append(f"TalentBrew Scrape: {err}")

    # Step 2d: Scrape Taleo portals
    if not feeds_only and not skip_scrape:
        cmd = [python, "main.py", "scrape", "--ats", "taleo", "--from-db"]
        if scrape_limit:
            cmd.extend(["--limit", str(scrape_limit)])
        if today_only:
            cmd.extend(["--today-only"])

        ok, err = run_step("Scrape Taleo portals", cmd)
        if not ok:
            errors.append(f"Taleo Scrape: {err}")

    # Step 2e: Scrape Oracle HCM portals
    if not feeds_only and not skip_scrape:
        cmd = [python, "main.py", "scrape", "--ats", "oracle", "--from-db"]
        if scrape_limit:
            cmd.extend(["--limit", str(scrape_limit)])
        if today_only:
            cmd.extend(["--today-only"])

        ok, err = run_step("Scrape Oracle HCM portals", cmd)
        if not ok:
            errors.append(f"Oracle Scrape: {err}")

        # Get total job count after all scrapes
        with db_session(DB_PATH) as conn:
            row = conn.execute("SELECT COUNT(*) as cnt FROM jobs").fetchone()
            jobs_found = row["cnt"]

    if feeds_only or skip_scrape:
        logger.info("Skipping scrape step")

    # Step 3: Generate feeds
    cmd = [python, "feed_generator.py"]
    ok, err = run_step("Generate filtered feeds", cmd)
    if not ok:
        errors.append(f"Feeds: {err}")
    else:
        feeds_generated = len(list(FEEDS_DIR.glob("*.yaml")))

    elapsed = time.time() - start

    # Record run result
    status = "completed" if not errors else "completed_with_errors"
    error_msg = "; ".join(errors) if errors else None

    with db_session(DB_PATH) as conn:
        finish_run(
            conn, run_id,
            portals_found=portals_found,
            jobs_found=jobs_found,
            feeds_generated=feeds_generated,
            status=status,
            error=error_msg,
        )

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETE in {elapsed:.1f}s")
    logger.info(f"  Status:          {status}")
    logger.info(f"  Portals in DB:   {portals_found}")
    logger.info(f"  Jobs in DB:      {jobs_found}")
    logger.info(f"  Feeds generated: {feeds_generated}")
    if errors:
        logger.warning(f"  Errors: {errors}")
    logger.info("=" * 60)


def show_status():
    """Show stats from the database and last pipeline run."""
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        portals = conn.execute("SELECT COUNT(*) as cnt FROM portals").fetchone()["cnt"]
        jobs = conn.execute("SELECT COUNT(*) as cnt FROM jobs").fetchone()["cnt"]
        nursing = conn.execute(
            "SELECT COUNT(*) as cnt FROM jobs WHERE is_nursing = 1"
        ).fetchone()["cnt"]
        sectors = conn.execute(
            "SELECT sector, COUNT(*) as cnt FROM portals GROUP BY sector ORDER BY cnt DESC"
        ).fetchall()

        last_run = conn.execute(
            "SELECT * FROM scrape_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()

    print(f"\n{'=' * 50}")
    print(f"NurseScraper Database Status")
    print(f"{'=' * 50}")
    print(f"  Database:     {DB_PATH}")
    print(f"  Portals:      {portals}")
    print(f"  Jobs:         {jobs}")
    print(f"  Nursing jobs: {nursing}")
    print(f"\n  Portals by sector:")
    for s in sectors:
        print(f"    {s['sector'] or 'unknown':<20} {s['cnt']}")

    if last_run:
        print(f"\n  Last pipeline run:")
        print(f"    Type:      {last_run['run_type']}")
        print(f"    Started:   {last_run['started_at']}")
        print(f"    Finished:  {last_run['finished_at'] or 'still running'}")
        print(f"    Status:    {last_run['status']}")
        if last_run["error"]:
            print(f"    Error:     {last_run['error']}")
    else:
        print(f"\n  No pipeline runs recorded yet.")

    feed_configs = list(FEEDS_DIR.glob("*.yaml"))
    print(f"\n  Feed configs: {len(feed_configs)}")
    for fc in feed_configs:
        print(f"    - {fc.stem}")

    print(f"{'=' * 50}\n")


def main():
    parser = argparse.ArgumentParser(description="NurseScraper Pipeline Orchestrator")
    sub = parser.add_subparsers(dest="command")

    daily_parser = sub.add_parser("daily", help="Run the full daily pipeline")
    daily_parser.add_argument("--skip-discovery", action="store_true")
    daily_parser.add_argument("--skip-scrape", action="store_true")
    daily_parser.add_argument("--feeds-only", action="store_true", help="Only regenerate feeds")
    daily_parser.add_argument("--discovery-workers", type=int, default=50)
    daily_parser.add_argument("--scrape-limit", type=int, default=None, help="Max portals to scrape")
    daily_parser.add_argument("--scrape-portal", default=None, help="Scrape a specific portal slug")
    daily_parser.add_argument("--all-dates", action="store_true", help="Include jobs from all dates (default: today only)")

    sub.add_parser("status", help="Show database and pipeline status")

    args = parser.parse_args()

    if args.command == "daily":
        run_daily(
            skip_discovery=args.skip_discovery,
            skip_scrape=args.skip_scrape,
            feeds_only=args.feeds_only,
            discovery_workers=args.discovery_workers,
            scrape_limit=args.scrape_limit,
            scrape_portal=args.scrape_portal,
            today_only=not args.all_dates,
        )
    elif args.command == "status":
        show_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
