"""
AllJobScraper — Multi-ATS Job Scraper CLI

Usage:
    python main.py scrape --ats icims [--portal uci] [--limit 5] [--offset 0]
    python main.py scrape --ats workday --from-db --offset 50 --limit 50
    python main.py discover --ats icims [--enum]
    python main.py scrape --ats icims --dry-run --today-only
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
from scrapers.talentbrew.scraper import TalentBrewScraper
from scrapers.taleo.scraper import TaleoScraper
from scrapers.oracle.scraper import OracleScraper
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
@click.option("--ats", required=True, type=click.Choice(["icims", "workday", "talentbrew", "taleo", "oracle"]))
@click.option("--portal", default=None, help="Scrape a specific portal by slug (e.g., 'uci')")
@click.option("--keyword", default=None, help="Search keyword filter (e.g., 'nurse')")
@click.option("--offset", default=0, type=int, help="Skip first N portals (for chunking)")
@click.option("--limit", default=None, type=int, help="Max number of portals to scrape")
@click.option("--dry-run", is_flag=True, help="Discover jobs but don't export")
@click.option("--output-dir", default=None, help="Override output directory")
@click.option("--from-db", is_flag=True, help="Load portals from SQLite DB instead of portals.yaml")
@click.option("--sector", default=None, help="Filter portals by sector (use with --from-db)")
@click.option("--today-only", is_flag=True, help="Only include jobs posted today")
@click.option("--skip-details", is_flag=True, help="Skip fetching individual job details (faster)")
@click.option("--max-detail-jobs", default=100, type=int, help="Max jobs per portal to fetch details for (default: 100)")
def scrape(ats: str, portal: str, keyword: str, offset: int, limit: int, dry_run: bool, output_dir: str, from_db: bool, sector: str, today_only: bool, skip_details: bool, max_detail_jobs: int):
    """Scrape job listings from ATS career portals."""
    logger = logging.getLogger("main")

    output_path = Path(output_dir) if output_dir else DATA_DIR / ats
    output_path.mkdir(parents=True, exist_ok=True)

    fetch_details = not skip_details
    if ats == "icims":
        _scrape_icims(portal, keyword, offset, limit, dry_run, output_path, logger, from_db=from_db, sector=sector, today_only=today_only, fetch_details=fetch_details, max_detail_jobs=max_detail_jobs)
    elif ats == "workday":
        _scrape_workday(portal, keyword, offset, limit, dry_run, output_path, logger, from_db=from_db, today_only=today_only, fetch_details=fetch_details, max_detail_jobs=max_detail_jobs)
    elif ats == "talentbrew":
        _scrape_talentbrew(portal, keyword, offset, limit, dry_run, output_path, logger, from_db=from_db, today_only=today_only, fetch_details=fetch_details, max_detail_jobs=max_detail_jobs)
    elif ats == "taleo":
        _scrape_taleo(portal, keyword, offset, limit, dry_run, output_path, logger, from_db=from_db, today_only=today_only, fetch_details=fetch_details, max_detail_jobs=max_detail_jobs)
    elif ats == "oracle":
        _scrape_oracle(portal, keyword, offset, limit, dry_run, output_path, logger, from_db=from_db, today_only=today_only, fetch_details=fetch_details, max_detail_jobs=max_detail_jobs)
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
    offset: int,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    sector: str | None = None,
    today_only: bool = False,
    fetch_details: bool = True,
    max_detail_jobs: int = 100,
):
    """Run the iCIMS scraper."""

    if from_db:
        companies = _load_companies_from_db(sector, logger)
    else:
        discovery = ICIMSDiscovery()
        companies = discovery.from_seed_list(str(PORTALS_FILE))

    # Filter to iCIMS only
    companies = [c for c in companies if c.ats_type == "icims"]

    # Filter to specific portal if requested
    if portal_slug:
        companies = [c for c in companies if c.ats_slug == portal_slug]
        if not companies:
            logger.error(f"Portal '{portal_slug}' not found")
            sys.exit(1)

    # Apply offset and limit for chunking
    if offset > 0:
        companies = companies[offset:]
    if limit:
        companies = companies[:limit]

    logger.info(f"Scraping {len(companies)} iCIMS portals")

    all_scraped_jobs = []

    for company in companies:
        try:
            scraper = ICIMSScraper(company)
            jobs = scraper.scrape_all(
                keyword=keyword,
                fetch_details=fetch_details,
                max_detail_jobs=max_detail_jobs,
                today_only=today_only,
            )
            all_scraped_jobs.extend(jobs)
            logger.info(f"✓ {company.name}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"✗ {company.name}: {e}")
            continue

    # Note: today_only filtering now happens INSIDE scrape_all() before detail fetch

    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
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


def _load_workday_portals_from_config(logger: logging.Logger) -> list[Company]:
    """Load Workday portals from config/portals.yaml."""
    import yaml
    from urllib.parse import urlparse

    companies = []
    try:
        with open(PORTALS_FILE) as f:
            data = yaml.safe_load(f)

        for entry in data.get("workday", []):
            url = entry.get("url", "")
            if not url:
                continue

            parsed = urlparse(url)
            hostname = parsed.hostname or ""
            parts = hostname.split(".")

            if len(parts) >= 3 and "myworkdayjobs" in hostname:
                tenant = parts[0]
                companies.append(Company(
                    name=entry.get("name", tenant.upper()),
                    portal_url=url,
                    ats_type="workday",
                    ats_slug=tenant,
                ))

        logger.info(f"Loaded {len(companies)} Workday portals from config")
    except Exception as e:
        logger.error(f"Failed to load Workday portals from config: {e}")

    return companies


def _load_workday_portals_from_db(logger: logging.Logger) -> list[Company]:
    """Load Workday portals from the SQLite database."""
    from storage.database import get_connection
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    rows = conn.execute(
        "SELECT * FROM portals WHERE ats_type = 'workday' AND verified = 1 ORDER BY name"
    ).fetchall()
    conn.close()

    companies = [Company.from_db_row(r) for r in rows]
    logger.info(f"Loaded {len(companies)} Workday portals from database")
    return companies


def _scrape_workday(
    portal_url: str | None,
    keyword: str | None,
    offset: int,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    today_only: bool = False,
    fetch_details: bool = True,
    max_detail_jobs: int = 100,
):
    """Run the Workday scraper."""
    from urllib.parse import urlparse

    # Load portals from db, config, or single URL
    if from_db:
        companies = _load_workday_portals_from_db(logger)
        if not companies:
            # Fall back to config if db is empty
            companies = _load_workday_portals_from_config(logger)
    elif portal_url:
        # Single portal URL provided
        parsed = urlparse(portal_url)
        hostname = parsed.hostname or ""
        parts = hostname.split(".")

        if len(parts) < 3 or "myworkdayjobs" not in hostname:
            logger.error(f"Invalid Workday URL: {portal_url}")
            logger.error("Expected format: https://{tenant}.{wd###}.myworkdayjobs.com/{site}")
            sys.exit(1)

        tenant = parts[0]
        companies = [Company(
            name=tenant.upper(),
            portal_url=portal_url,
            ats_type="workday",
            ats_slug=tenant,
        )]
    else:
        # Load from config file
        companies = _load_workday_portals_from_config(logger)

    if not companies:
        logger.warning("No Workday portals to scrape")
        return

    # Apply offset and limit for chunking
    if offset > 0:
        companies = companies[offset:]
    if limit:
        companies = companies[:limit]

    logger.info(f"Scraping {len(companies)} Workday portals")

    all_scraped_jobs = []

    for company in companies:
        try:
            scraper = WorkdayScraper(company)
            # today_only filtering happens INSIDE scrape_all, before detail fetch
            jobs = scraper.scrape_all(
                keyword=keyword,
                fetch_details=fetch_details,
                max_detail_jobs=max_detail_jobs,
                today_only=today_only,
            )
            all_scraped_jobs.extend(jobs)
            logger.info(f"✓ {company.name}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"✗ {company.name}: {e}")
            continue

    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        for company in companies:
            portal_id = upsert_portal(
                conn,
                subdomain=company.ats_slug,
                slug=company.ats_slug,
                name=company.name,
                url=company.portal_url,
                ats_type="workday",
                verified=True,
            )
            company_jobs = [j for j in all_scraped_jobs if j.company_name == company.name]
            for job in company_jobs:
                job.save_to_db(conn, portal_id)
        logger.info(f"Saved {len(all_scraped_jobs)} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in all_scraped_jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name} — {job.location}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(all_scraped_jobs, output_path, f"workday_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(all_scraped_jobs, output_path, f"workday_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


def _load_talentbrew_portals_from_config(logger: logging.Logger) -> list[Company]:
    """Load TalentBrew portals from config/portals.yaml."""
    companies = []
    try:
        with open(PORTALS_FILE) as f:
            data = yaml.safe_load(f)

        for entry in data.get("talentbrew", []):
            url = entry.get("url", "")
            if not url:
                continue

            companies.append(Company(
                name=entry.get("name", ""),
                portal_url=url,
                ats_type="talentbrew",
                ats_slug=entry.get("slug", ""),
                sector=entry.get("sector", ""),
                state=entry.get("state", ""),
            ))

        logger.info(f"Loaded {len(companies)} TalentBrew portals from config")
    except Exception as e:
        logger.error(f"Failed to load TalentBrew portals from config: {e}")

    return companies


def _load_talentbrew_portals_from_db(logger: logging.Logger) -> list[Company]:
    """Load TalentBrew portals from the SQLite database."""
    from storage.database import get_connection
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    rows = conn.execute(
        "SELECT * FROM portals WHERE ats_type = 'talentbrew' AND verified = 1 ORDER BY name"
    ).fetchall()
    conn.close()

    companies = [Company.from_db_row(r) for r in rows]
    logger.info(f"Loaded {len(companies)} TalentBrew portals from database")
    return companies


def _scrape_talentbrew(
    portal_slug: str | None,
    keyword: str | None,
    offset: int,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    today_only: bool = False,
    fetch_details: bool = True,
    max_detail_jobs: int = 100,
):
    """Run the TalentBrew scraper."""

    # Load portals from db or config
    if from_db:
        companies = _load_talentbrew_portals_from_db(logger)
        if not companies:
            # Fall back to config if db is empty
            companies = _load_talentbrew_portals_from_config(logger)
    else:
        companies = _load_talentbrew_portals_from_config(logger)

    # Filter to specific portal if requested
    if portal_slug:
        companies = [c for c in companies if c.ats_slug == portal_slug]
        if not companies:
            logger.error(f"Portal '{portal_slug}' not found")
            sys.exit(1)

    if not companies:
        logger.warning("No TalentBrew portals to scrape")
        return

    # Apply offset and limit for chunking
    if offset > 0:
        companies = companies[offset:]
    if limit:
        companies = companies[:limit]

    logger.info(f"Scraping {len(companies)} TalentBrew portals")

    all_scraped_jobs = []

    for company in companies:
        try:
            scraper = TalentBrewScraper(company)
            jobs = scraper.scrape_all(
                keyword=keyword,
                fetch_details=fetch_details,
                max_detail_jobs=max_detail_jobs,
                today_only=today_only,
            )
            all_scraped_jobs.extend(jobs)
            logger.info(f"✓ {company.name}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"✗ {company.name}: {e}")
            continue

    # Note: today_only filtering now happens INSIDE scrape_all() before detail fetch

    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        for company in companies:
            portal_id = upsert_portal(
                conn,
                subdomain=company.ats_slug or company.name.lower().replace(" ", "-"),
                slug=company.ats_slug or company.name.lower().replace(" ", "-"),
                name=company.name,
                url=company.portal_url,
                ats_type="talentbrew",
                verified=True,
            )
            company_jobs = [j for j in all_scraped_jobs if j.company_name == company.name]
            for job in company_jobs:
                job.save_to_db(conn, portal_id)
        logger.info(f"Saved {len(all_scraped_jobs)} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in all_scraped_jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name} — {job.location}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(all_scraped_jobs, output_path, f"talentbrew_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(all_scraped_jobs, output_path, f"talentbrew_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


def _load_taleo_portals_from_config(logger: logging.Logger) -> list[dict]:
    """Load Taleo portals from config/portals.yaml."""
    portals = []
    try:
        with open(PORTALS_FILE) as f:
            data = yaml.safe_load(f)

        for entry in data.get("taleo", []):
            url = entry.get("url", "")
            if not url:
                continue

            portals.append({
                "name": entry.get("name", ""),
                "url": url,
                "career_section": entry.get("career_section", "jobsearch"),
                "slug": entry.get("slug", ""),
                "sector": entry.get("sector", ""),
                "state": entry.get("state", ""),
            })

        logger.info(f"Loaded {len(portals)} Taleo portals from config")
    except Exception as e:
        logger.error(f"Failed to load Taleo portals from config: {e}")

    return portals


def _load_taleo_portals_from_db(logger: logging.Logger) -> list[dict]:
    """Load Taleo portals from the SQLite database."""
    from storage.database import get_connection
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    rows = conn.execute(
        "SELECT * FROM portals WHERE ats_type = 'taleo' AND verified = 1 ORDER BY name"
    ).fetchall()
    conn.close()

    columns = [desc[0] for desc in rows[0].keys()] if rows else []
    portals = [{
        "name": r["name"],
        "url": r["url"],
        "career_section": r["career_section"] if "career_section" in columns else "jobsearch",
        "slug": r["slug"],
    } for r in rows]
    logger.info(f"Loaded {len(portals)} Taleo portals from database")
    return portals


def _scrape_taleo(
    portal_slug: str | None,
    keyword: str | None,
    offset: int,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    today_only: bool = False,
    fetch_details: bool = True,
    max_detail_jobs: int = 100,
):
    """Run the Taleo scraper."""
    from urllib.parse import urlparse

    # Load portals from db or config
    if from_db:
        portals = _load_taleo_portals_from_db(logger)
        if not portals:
            portals = _load_taleo_portals_from_config(logger)
    else:
        portals = _load_taleo_portals_from_config(logger)

    # Filter to specific portal if requested
    if portal_slug:
        portals = [p for p in portals if p.get("slug") == portal_slug]
        if not portals:
            logger.error(f"Portal '{portal_slug}' not found")
            sys.exit(1)

    if not portals:
        logger.warning("No Taleo portals to scrape")
        return

    # Apply offset and limit for chunking
    if offset > 0:
        portals = portals[offset:]
    if limit:
        portals = portals[:limit]

    logger.info(f"Scraping {len(portals)} Taleo portals")

    all_scraped_jobs = []

    for portal in portals:
        try:
            # Create Company object for the scraper
            company = Company(
                name=portal["name"],
                portal_url=portal["url"],
                ats_type="taleo",
                ats_slug=portal.get("slug", ""),
            )

            scraper = TaleoScraper(company)
            jobs = scraper.scrape_all(
                keyword=keyword,
                fetch_details=fetch_details,
                max_detail_jobs=max_detail_jobs,
                today_only=today_only,
            )

            all_scraped_jobs.extend(jobs)
            logger.info(f"✓ {portal['name']}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"✗ {portal['name']}: {e}")
            continue

    # Note: today_only filtering now happens INSIDE scrape_all() before detail fetch

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        for portal in portals:
            portal_id = upsert_portal(
                conn,
                subdomain=portal.get("slug", portal["name"].lower().replace(" ", "-")),
                slug=portal.get("slug", portal["name"].lower().replace(" ", "-")),
                name=portal["name"],
                url=portal["url"],
                ats_type="taleo",
                verified=True,
            )
            portal_jobs = [j for j in all_scraped_jobs if j.company_name == portal["name"]]
            for job in portal_jobs:
                job.save_to_db(conn, portal_id)
        logger.info(f"Saved {len(all_scraped_jobs)} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in all_scraped_jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(all_scraped_jobs, output_path, f"taleo_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(all_scraped_jobs, output_path, f"taleo_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


def _load_oracle_portals_from_config(logger: logging.Logger) -> list[dict]:
    """Load Oracle HCM portals from config/portals.yaml."""
    portals = []
    try:
        with open(PORTALS_FILE) as f:
            data = yaml.safe_load(f)

        for entry in data.get("oracle", []):
            url = entry.get("url", "")
            if not url:
                continue

            portals.append({
                "name": entry.get("name", ""),
                "url": url,
                "site_number": entry.get("site_number", "CX_1"),
                "slug": entry.get("slug", ""),
                "sector": entry.get("sector", ""),
                "state": entry.get("state", ""),
            })

        logger.info(f"Loaded {len(portals)} Oracle HCM portals from config")
    except Exception as e:
        logger.error(f"Failed to load Oracle HCM portals from config: {e}")

    return portals


def _load_oracle_portals_from_db(logger: logging.Logger) -> list[dict]:
    """Load Oracle HCM portals from the SQLite database."""
    from storage.database import get_connection
    init_db(DB_PATH)
    conn = get_connection(DB_PATH)

    rows = conn.execute(
        "SELECT * FROM portals WHERE ats_type = 'oracle' AND verified = 1 ORDER BY name"
    ).fetchall()
    conn.close()

    columns = [desc[0] for desc in rows[0].keys()] if rows else []
    portals = [{
        "name": r["name"],
        "url": r["url"],
        "site_number": r["site_number"] if "site_number" in columns else "CX_1",
        "slug": r["slug"],
    } for r in rows]
    logger.info(f"Loaded {len(portals)} Oracle HCM portals from database")
    return portals


def _scrape_oracle(
    portal_slug: str | None,
    keyword: str | None,
    offset: int,
    limit: int | None,
    dry_run: bool,
    output_path: Path,
    logger: logging.Logger,
    from_db: bool = False,
    today_only: bool = False,
    fetch_details: bool = True,
    max_detail_jobs: int = 100,
):
    """Run the Oracle HCM scraper."""
    from urllib.parse import urlparse

    # Load portals from db or config
    if from_db:
        portals = _load_oracle_portals_from_db(logger)
        if not portals:
            portals = _load_oracle_portals_from_config(logger)
    else:
        portals = _load_oracle_portals_from_config(logger)

    # Filter to specific portal if requested
    if portal_slug:
        portals = [p for p in portals if p.get("slug") == portal_slug]
        if not portals:
            logger.error(f"Portal '{portal_slug}' not found")
            sys.exit(1)

    if not portals:
        logger.warning("No Oracle HCM portals to scrape")
        return

    # Apply offset and limit for chunking
    if offset > 0:
        portals = portals[offset:]
    if limit:
        portals = portals[:limit]

    logger.info(f"Scraping {len(portals)} Oracle HCM portals")

    all_scraped_jobs = []

    for portal in portals:
        try:
            # Create Company object for the scraper
            company = Company(
                name=portal["name"],
                portal_url=portal["url"],
                ats_type="oracle",
                ats_slug=portal.get("slug", ""),
            )

            scraper = OracleScraper(company)
            jobs = scraper.scrape_all(
                keyword=keyword,
                fetch_details=fetch_details,
                max_detail_jobs=max_detail_jobs,
                today_only=today_only,
            )

            all_scraped_jobs.extend(jobs)
            logger.info(f"✓ {portal['name']}: {len(jobs)} jobs")
        except Exception as e:
            logger.error(f"✗ {portal['name']}: {e}")
            continue

    # Note: today_only filtering now happens INSIDE scrape_all() before detail fetch

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"Total jobs scraped: {len(all_scraped_jobs)}")
    logger.info(f"{'='*50}")

    # Save to SQLite database
    init_db(DB_PATH)
    with db_session(DB_PATH) as conn:
        for portal in portals:
            portal_id = upsert_portal(
                conn,
                subdomain=portal.get("slug", portal["name"].lower().replace(" ", "-")),
                slug=portal.get("slug", portal["name"].lower().replace(" ", "-")),
                name=portal["name"],
                url=portal["url"],
                ats_type="oracle",
                verified=True,
            )
            portal_jobs = [j for j in all_scraped_jobs if j.company_name == portal["name"]]
            for job in portal_jobs:
                job.save_to_db(conn, portal_id)
        logger.info(f"Saved {len(all_scraped_jobs)} jobs to SQLite database")

    if dry_run:
        logger.info("Dry run — skipping file export")
        for job in all_scraped_jobs[:5]:
            logger.info(f"  [{job.id}] {job.title} @ {job.company_name}")
        return

    # Export to flat files
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if OUTPUT_FORMAT in ("csv", "both"):
        csv_path = export_to_csv(all_scraped_jobs, output_path, f"oracle_jobs_{date_str}.csv")
        logger.info(f"CSV exported to {csv_path}")

    if OUTPUT_FORMAT in ("json", "both"):
        json_path = export_to_json(all_scraped_jobs, output_path, f"oracle_jobs_{date_str}.json")
        logger.info(f"JSON exported to {json_path}")


@cli.command()
@click.option("--ats", required=True, type=click.Choice(["icims", "workday", "talentbrew", "taleo", "oracle"]))
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
