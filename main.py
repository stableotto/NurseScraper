"""
NurseScraper — ATS Job Scraper CLI

Usage:
    python main.py scrape --ats icims [--portal uci] [--all-jobs] [--limit 5]
    python main.py discover --ats icims [--enum]
    python main.py scrape --ats icims --dry-run
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

import click
import yaml

from config.settings import (
    DATA_DIR,
    DB_PATH,
    PORTALS_FILE,
    LOG_LEVEL,
    OUTPUT_FORMAT,
)
from models.company import Company
from scrapers.icims.scraper import ICIMSScraper
from scrapers.icims.discovery import ICIMSDiscovery
from scrapers.workday.scraper import WorkdayScraper
from storage.export import export_to_csv, export_to_json
from storage.database import init_db, db_session, get_portal_id, upsert_portal

# ──────────────────────────────────────────────
# Logging setup
# ──────────────────────────────────────────────

def setup_logging(level: str = LOG_LEVEL) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose: bool):
    """NurseScraper — ATS Job Scraper for Healthcare"""
    setup_logging("DEBUG" if verbose else LOG_LEVEL)


@cli.command()
@click.option("--ats", required=True, type=click.Choice(["icims", "workday", "taleo", "oracle"]))
@click.option("--portal", default=None, help="Scrape a specific portal by slug (e.g., 'uci')")
@click.option("--keyword", default=None, help="Search keyword filter (e.g., 'nurse')")
@click.option("--all-jobs", is_flag=True, help="Include non-nursing jobs")
@click.option("--limit", default=None, type=int, help="Max number of portals to scrape")
@click.option("--dry-run", is_flag=True, help="Discover jobs but don't export")
@click.option("--output-dir", default=None, help="Override output directory")
@click.option("--from-db", is_flag=True, help="Load portals from SQLite DB instead of portals.yaml")
@click.option("--sector", default=None, help="Filter portals by sector (use with --from-db)")
@click.option("--today-only", is_flag=True, help="Only include jobs posted today")
def scrape(ats: str, portal: str, keyword: str, all_jobs: bool, limit: int, dry_run: bool, output_dir: str, from_db: bool, sector: str, today_only: bool):
    """Scrape job listings from ATS career portals."""
    logger = logging.getLogger("main")

    output_path = Path(output_dir) if output_dir else DATA_DIR / ats
    output_path.mkdir(parents=True, exist_ok=True)

    if ats == "icims":
        _scrape_icims(portal, keyword, all_jobs, limit, dry_run, output_path, logger, from_db=from_db, sector=sector, today_only=today_only)
    elif ats == "workday":
        _scrape_workday(portal, keyword, all_jobs, limit, dry_run, output_path, logger, today_only=today_only)
    else:
        logger.error(f"ATS '{ats}' scraper not yet implemented. Coming soon!")
        sys.exit(1)


def _load_companies_from_db(sector: str | None, logger: logging.Logger) -> list[Company]:
    """Load Company objects from the SQLite database, optionally filtered by sector."""
    from storage.database import get_connection
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    sql = "SELECT * FROM portals WHERE verified = 1"
    params: list = []
    if sector:
        # Support comma-separated sectors and healthcare-like aliases
        sectors = [s.strip() for s in sector.split(",")]
        healthcare_sectors = {"healthcare", "hospital", "health_system", "university_hospital",
                              "medical_group", "childrens"}
        expanded = set()
        for s in sectors:
            if s == "healthcare":
                expanded |= healthcare_sectors
            else:
                expanded.add(s)
        placeholders = ",".join("?" * len(expanded))
        sql += f" AND sector IN ({placeholders})"
        params.extend(expanded)

    sql += " ORDER BY name"
    rows = conn.execute(sql, params).fetchall()
    conn.close()

    companies = [Company.from_db_row(r) for r in rows]
    logger.info(f"Loaded {len(companies)} portals from database" +
                (f" (sector={sector})" if sector else ""))
    return companies


def _filter_jobs_by_date(jobs: list, today_only: bool, logger: logging.Logger) -> list:
    """Filter jobs to only include those posted today or within last 24 hours."""
    if not today_only:
        return jobs

    from datetime import date, datetime, timedelta
    today = date.today()
    yesterday = today - timedelta(days=1)
    filtered = []

    for job in jobs:
        dominated = False

        # Check if job was posted today or yesterday via posted_date field
        if job.posted_date:
            job_date = job.posted_date.date()
            if job_date == today or job_date == yesterday:
                filtered.append(job)
                continue

        # Check postedOn text for "Posted Today" or "Posted Yesterday"
        raw = job.raw_data or {}

        # Workday format: raw_data has listing.posted_on or jobPostingInfo.postedOn
        posted_on = ""
        if "listing" in raw:
            posted_on = raw["listing"].get("posted_on", "")
        elif "jobPostingInfo" in raw:
            posted_on = raw["jobPostingInfo"].get("postedOn", "")
        else:
            posted_on = raw.get("postedOn", "") or raw.get("posted_on", "")

        # iCIMS Jibe format: raw_data has posted_date or publish_date
        if not posted_on:
            posted_on = raw.get("posted_date", "") or raw.get("publish_date", "")

        posted_on_lower = posted_on.lower() if posted_on else ""
        if "today" in posted_on_lower or "yesterday" in posted_on_lower:
            filtered.append(job)

    logger.info(f"Filtered to {len(filtered)} recent jobs (from {len(jobs)} total)")
    return filtered


def _scrape_icims(
    portal_slug: str | None,
    keyword: str | None,
    all_jobs: bool,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    sector: str | None = None,
    today_only: bool = False,
):
    """Run the iCIMS scraper."""
    if from_db:
        companies = _load_companies_from_db(sector, logger)
    else:
        discovery = ICIMSDiscovery()
        companies = discovery.from_seed_list(str(PORTALS_FILE))

    # Filter to specific portal if requested
    if portal_slug:
        companies = [c for c in companies if c.ats_slug == portal_slug]
        if not companies:
            logger.error(f"Portal '{portal_slug}' not found")
            sys.exit(1)

    # Apply limit
    if limit:
        companies = companies[:limit]

    logger.info(f"Scraping {len(companies)} iCIMS portals")

    all_scraped_jobs = []

    for company in companies:
        try:
            scraper = ICIMSScraper(company)
            jobs = scraper.scrape_all(
                keyword=keyword,
                nursing_only=not all_jobs,
            )
            all_scraped_jobs.extend(jobs)
            logger.info(
                f"✓ {company.name}: {len(jobs)} jobs "
                f"({'all' if all_jobs else 'nursing only'})"
            )
        except Exception as e:
            logger.error(f"✗ {company.name}: {e}")
            continue

    # Filter by date if requested
    if today_only:
        all_scraped_jobs = _filter_jobs_by_date(all_scraped_jobs, today_only, logger)

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
    logger.info(f"Nursing jobs: {sum(1 for j in all_scraped_jobs if j.is_nursing)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        db_jobs_saved = 0
        for company in companies:
            portal_id = company.save_to_db(conn)
            company_jobs = [j for j in all_scraped_jobs if j.company_name == company.name]
            for job in company_jobs:
                job.save_to_db(conn, portal_id)
                db_jobs_saved += 1
        logger.info(f"Saved {db_jobs_saved} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in all_scraped_jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name} — {job.location}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(all_scraped_jobs, output_path, f"icims_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(all_scraped_jobs, output_path, f"icims_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


def _scrape_workday(
    portal_url: str | None,
    keyword: str | None,
    all_jobs: bool,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    today_only: bool = False,
):
    """Run the Workday scraper."""
    if not portal_url:
        logger.error("Workday scraper requires --portal with a full URL")
        logger.error("Example: --portal https://rch.wd108.myworkdayjobs.com/Careers")
        sys.exit(1)

    # Create a Company object from the URL
    from urllib.parse import urlparse
    parsed = urlparse(portal_url)
    hostname = parsed.hostname or ""
    parts = hostname.split(".")

    if len(parts) < 3 or "myworkdayjobs" not in hostname:
        logger.error(f"Invalid Workday URL: {portal_url}")
        logger.error("Expected format: https://{tenant}.{wd###}.myworkdayjobs.com/{site}")
        sys.exit(1)

    tenant = parts[0]
    company = Company(
        name=tenant.upper(),
        portal_url=portal_url,
        ats_type="workday",
        ats_slug=tenant,
    )

    logger.info(f"Scraping Workday portal: {portal_url}")

    try:
        scraper = WorkdayScraper(company)
        jobs = scraper.scrape_all(
            keyword=keyword,
            nursing_only=not all_jobs,
        )
        logger.info(
            f"✓ {company.name}: {len(jobs)} jobs "
            f"({'all' if all_jobs else 'nursing only'})"
        )
    except Exception as e:
        logger.error(f"✗ {company.name}: {e}")
        sys.exit(1)

    # Filter by date if requested
    if today_only:
        jobs = _filter_jobs_by_date(jobs, today_only, logger)

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(jobs)}")
    logger.info(f"Nursing jobs: {sum(1 for j in jobs if j.is_nursing)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        portal_id = upsert_portal(
            conn,
            subdomain=company.ats_slug,
            slug=company.ats_slug,
            name=company.name,
            url=company.portal_url,
            ats_type="workday",
            verified=True,
        )
        for job in jobs:
            job.save_to_db(conn, portal_id)
        logger.info(f"Saved {len(jobs)} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name} — {job.location}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(jobs, output_path, f"workday_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(jobs, output_path, f"workday_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


@cli.command()
@click.option("--ats", required=True, type=click.Choice(["icims", "workday", "taleo", "oracle"]))
@click.option("--enum", is_flag=True, help="Run subdomain enumeration (slower)")
@click.option("--output", default=None, help="Save discovered portals to YAML/JSON")
def discover(ats: str, enum: bool, output: str):
    """Discover companies using a specific ATS platform."""
    logger = logging.getLogger("main")

    if ats == "icims":
        disc = ICIMSDiscovery()
        companies = disc.discover_all(
            yaml_path=str(PORTALS_FILE),
            run_subdomain_enum=enum,
        )

        logger.info(f"\nDiscovered {len(companies)} iCIMS portals:")
        for c in companies:
            status = "✓ verified" if c.verified else "? unverified"
            logger.info(f"  [{status}] {c.name} — {c.portal_url} (slug: {c.ats_slug})")

        if output:
            out_path = Path(output)
            if out_path.suffix in (".yaml", ".yml"):
                entries = [c.to_dict() for c in companies]
                with open(out_path, "w") as f:
                    yaml.dump({"icims": entries}, f, default_flow_style=False)
                logger.info(f"Saved to {out_path}")
    else:
        logger.error(f"Discovery for '{ats}' not yet implemented")
        sys.exit(1)


if __name__ == "__main__":
    cli()
