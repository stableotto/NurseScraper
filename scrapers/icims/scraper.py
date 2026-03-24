"""
iCIMS Job Scraper

Scrapes job listings from iCIMS-powered career portals.
Supports two modes:
  1. Jibe API (preferred) — clean JSON API at {portal}/api/jobs
  2. Raw iCIMS portal (fallback) — HTML scraping of careers-{slug}.icims.com
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

from models.job import Job
from models.company import Company
from scrapers.base import BaseScraper
from scrapers.icims.config import (
    JIBE_API_PATH,
    ICIMS_SEARCH_PATH,
    DEFAULT_PAGE_SIZE,
    MAX_PAGES,
    NURSING_CATEGORIES,
)

logger = logging.getLogger(__name__)


class ICIMSScraper(BaseScraper):
    """Scraper for iCIMS career portals (with Jibe frontend support)."""

    ATS_NAME = "icims"

    def __init__(self, company: Company, **kwargs):
        super().__init__(company, **kwargs)
        self._api_mode: str | None = None  # "jibe" or "icims_raw"
        self._jibe_domain: str = ""

    # ──────────────────────────────────────────────
    # API Mode Detection
    # ──────────────────────────────────────────────

    def _detect_api_mode(self) -> str:
        """Probe the portal to determine if it uses the Jibe API or raw iCIMS."""
        portal_url = self.company.portal_url.rstrip("/")

        # Try Jibe API first
        jibe_url = f"{portal_url}{JIBE_API_PATH}"
        try:
            resp = self._get(jibe_url, params={"page": 1})
            data = resp.json()
            # Jibe API returns a dict with "jobs" key or similar
            if isinstance(data, (dict, list)):
                self._api_mode = "jibe"
                # Try to extract the jibe domain from the response or portal URL
                parsed = urlparse(portal_url)
                self._jibe_domain = parsed.hostname or ""
                logger.info(f"Detected Jibe API mode for {self.company.name}")
                return "jibe"
        except Exception as e:
            logger.debug(f"Jibe API not available at {jibe_url}: {e}")

        # Fall back to raw iCIMS
        self._api_mode = "icims_raw"
        logger.info(f"Using raw iCIMS mode for {self.company.name}")
        return "icims_raw"

    # ──────────────────────────────────────────────
    # Jibe API Mode
    # ──────────────────────────────────────────────

    def _fetch_jibe_page(
        self,
        page: int = 1,
        keyword: Optional[str] = None,
        category: Optional[str] = None,
    ) -> dict:
        """Fetch a single page from the Jibe API."""
        portal_url = self.company.portal_url.rstrip("/")
        api_url = f"{portal_url}{JIBE_API_PATH}"

        params = {"page": page}

        # Add jibe domain if we detected it
        if self._jibe_domain:
            # The domain param expects the jibeapply.com domain,
            # but we pass the portal domain and let Jibe resolve it
            params["domain"] = self._jibe_domain

        if keyword:
            params["query"] = keyword
        if category:
            params["categories"] = category

        resp = self._get(api_url, params=params)
        return resp.json()

    def _parse_jibe_jobs(self, data: dict) -> list[Job]:
        """Parse jobs from a Jibe API response."""
        jobs = []

        # Handle different response shapes
        job_list = []
        if isinstance(data, dict):
            job_list = data.get("jobs", data.get("results", []))
            # Some Jibe APIs nest jobs inside "data"
            if not job_list and "data" in data:
                inner = data["data"]
                if isinstance(inner, dict):
                    job_list = inner.get("jobs", inner.get("results", []))
                elif isinstance(inner, list):
                    job_list = inner
        elif isinstance(data, list):
            job_list = data

        for item in job_list:
            try:
                job = self._jibe_item_to_job(item)
                jobs.append(job)
            except Exception as e:
                logger.warning(f"Failed to parse Jibe job item: {e}")
                continue

        return jobs

    def _jibe_item_to_job(self, item: dict) -> Job:
        """Convert a single Jibe API job item to our Job model."""
        # Jibe API often nests all fields under "data" key
        raw_item = item
        if "data" in item and isinstance(item["data"], dict):
            item = item["data"]

        # Extract common Jibe fields (field names vary slightly by portal)
        job_id = str(
            item.get("req_id")
            or item.get("id")
            or item.get("jobId")
            or item.get("requisitionId", "")
        )

        title = item.get("title", item.get("name", ""))

        # Location — can be a string or structured fields
        location = item.get("location_name", "")
        if not location:
            city = item.get("city", "")
            state = item.get("state", "")
            location = f"{city}, {state}".strip(", ") if city or state else ""

        # Department
        department = item.get("department", item.get("business_unit", ""))
        if isinstance(department, dict):
            department = department.get("name", str(department))

        # Job type
        job_type = item.get("employment_type", item.get("type", ""))
        if isinstance(job_type, dict):
            job_type = job_type.get("name", str(job_type))

        # Posted date
        posted_date = None
        date_str = item.get("posted_date", item.get("publish_date", item.get("created_at", "")))
        if date_str:
            try:
                posted_date = parse_date(str(date_str))
            except (ValueError, TypeError):
                pass

        # URL — build from portal URL + slug
        slug = item.get("slug", item.get("url", ""))
        if slug and not slug.startswith("http"):
            portal_url = self.company.portal_url.rstrip("/")
            url = f"{portal_url}/jobs/{slug}" if not slug.startswith("/") else f"{portal_url}{slug}"
        elif slug:
            url = slug
        else:
            portal_url = self.company.portal_url.rstrip("/")
            url = f"{portal_url}/jobs/{job_id}"

        # Description (often HTML)
        description = item.get("description", "")
        if description:
            description = self._strip_html(description)

        # Qualifications
        qualifications = item.get("qualifications", item.get("requirements", ""))
        if qualifications:
            qualifications = self._strip_html(qualifications)

        # Salary
        salary = item.get("salary_range", "")
        if not salary:
            # Check tags — some portals put salary in tags
            for key in ("tags1", "tags2", "tags3", "tags4"):
                tag_val = item.get(key, "")
                if tag_val and any(c in str(tag_val).lower() for c in ("$", "salary", "pay", "hour", "year")):
                    salary = str(tag_val)
                    break

        # Categories
        categories = []
        for key in ("categories", "category", "tags1", "tags2"):
            cat = item.get(key)
            if isinstance(cat, list):
                categories.extend([str(c) for c in cat])
            elif isinstance(cat, str) and cat:
                categories.append(cat)

        return Job(
            id=job_id,
            source_ats="icims",
            company_name=self.company.name,
            title=title,
            department=department,
            location=location,
            job_type=job_type,
            posted_date=posted_date,
            url=url,
            description=description,
            qualifications=qualifications,
            salary_range=salary if salary else None,
            categories=categories,
            raw_data=raw_item,
        )

    # ──────────────────────────────────────────────
    # Raw iCIMS Mode (fallback)
    # ──────────────────────────────────────────────

    def _build_icims_url(self) -> str:
        """Build the raw iCIMS career portal URL."""
        # Use portal_url if available (already has correct format)
        if self.company.portal_url and "icims.com" in self.company.portal_url:
            return self.company.portal_url.rstrip("/")

        # Fallback: build from slug, stripping "careers-" prefix if present
        slug = self.company.ats_slug
        if slug.startswith("careers-"):
            slug = slug[8:]  # Remove "careers-" prefix
        return f"https://careers-{slug}.icims.com"

    def _fetch_icims_search_page(
        self,
        page: int = 1,
        keyword: Optional[str] = None,
    ) -> str:
        """Fetch a search results page from the raw iCIMS portal."""
        base = self._build_icims_url()
        url = f"{base}{ICIMS_SEARCH_PATH}"

        params = {
            "ss": "1",
            "in_iframe": "1",
            "pr": page,
        }
        if keyword:
            params["searchKeyword"] = keyword
            params["searchRelation"] = "keyword_all"

        resp = self._get(url, params=params)
        return resp.text

    def _parse_icims_search_page(self, html: str) -> list[Job]:
        """Parse job listings from raw iCIMS search results HTML."""
        soup = BeautifulSoup(html, "lxml")
        jobs = []

        # iCIMS uses various listing classes
        job_rows = soup.select(".iCIMS_JobsTable .row, .listingContainer, [class*='job']")
        if not job_rows:
            # Try finding links to job pages
            job_links = soup.find_all("a", href=re.compile(r"/jobs/\d+/"))
            for link in job_links:
                job_id_match = re.search(r"/jobs/(\d+)/", link.get("href", ""))
                if job_id_match:
                    job_id = job_id_match.group(1)
                    title = link.get_text(strip=True)
                    base = self._build_icims_url()
                    jobs.append(
                        Job(
                            id=job_id,
                            source_ats="icims",
                            company_name=self.company.name,
                            title=title,
                            url=f"{base}/jobs/{job_id}/job",
                        )
                    )
        else:
            for row in job_rows:
                try:
                    link = row.find("a", href=re.compile(r"/jobs/\d+"))
                    if not link:
                        continue
                    job_id_match = re.search(r"/jobs/(\d+)", link.get("href", ""))
                    if not job_id_match:
                        continue

                    job_id = job_id_match.group(1)
                    title = link.get_text(strip=True)
                    base = self._build_icims_url()

                    # Try to get location from the row
                    location = ""
                    loc_el = row.select_one(".iCIMS_JobLocation, .location, [data-field='location']")
                    if loc_el:
                        location = loc_el.get_text(strip=True)

                    jobs.append(
                        Job(
                            id=job_id,
                            source_ats="icims",
                            company_name=self.company.name,
                            title=title,
                            location=location,
                            url=f"{base}/jobs/{job_id}/job",
                        )
                    )
                except Exception as e:
                    logger.warning(f"Failed to parse iCIMS row: {e}")
                    continue

        return jobs

    def _fetch_icims_job_detail(self, job: Job) -> Job:
        """Fetch full details from an individual iCIMS job page."""
        try:
            resp = self._get(job.url)
            soup = BeautifulSoup(resp.text, "lxml")

            # Title
            title_el = soup.select_one(".iCIMS_Header h1, .header h1, [class*='title'] h1")
            if title_el:
                job.title = title_el.get_text(strip=True)

            # Description
            desc_el = soup.select_one(
                ".iCIMS_InfoMsg_Job, .job-description, [class*='description']"
            )
            if desc_el:
                job.description = self._strip_html(desc_el.decode_contents())

            # Other fields
            for field_el in soup.select(".iCIMS_InfoField"):
                label = field_el.select_one(".iCIMS_InfoField_Label")
                value = field_el.select_one(".iCIMS_InfoField_Value")
                if label and value:
                    label_text = label.get_text(strip=True).lower()
                    value_text = value.get_text(strip=True)

                    if "location" in label_text:
                        job.location = value_text
                    elif "type" in label_text or "schedule" in label_text:
                        job.job_type = value_text
                    elif "department" in label_text:
                        job.department = value_text
                    elif "salary" in label_text or "pay" in label_text or "compensation" in label_text:
                        job.salary_range = value_text
                    elif "posted" in label_text or "date" in label_text:
                        try:
                            job.posted_date = parse_date(value_text)
                        except (ValueError, TypeError):
                            pass

            # Extract salary from description if not found in fields
            if not job.salary_range and job.description:
                job.salary_range = self.extract_salary_from_text(job.description)

        except Exception as e:
            logger.error(f"Failed to fetch iCIMS job detail for {job.id}: {e}")

        return job

    # ──────────────────────────────────────────────
    # Public Interface (implements BaseScraper)
    # ──────────────────────────────────────────────

    def discover_jobs(self, keyword: Optional[str] = None, **kwargs) -> list[Job]:
        """Discover all job listings from this portal."""
        if self._api_mode is None:
            self._detect_api_mode()

        if self._api_mode == "jibe":
            return self._discover_jobs_jibe(keyword)
        else:
            return self._discover_jobs_icims_raw(keyword)

    def _discover_jobs_jibe(self, keyword: Optional[str] = None) -> list[Job]:
        """Fetch all jobs using the Jibe API (paginated)."""
        all_jobs = []
        page = 1

        while page <= MAX_PAGES:
            logger.debug(f"Fetching Jibe page {page}...")
            try:
                data = self._fetch_jibe_page(page=page, keyword=keyword)
            except Exception as e:
                logger.error(f"Failed to fetch Jibe page {page}: {e}")
                break

            jobs = self._parse_jibe_jobs(data)
            if not jobs:
                logger.debug(f"No jobs on page {page}, stopping pagination")
                break

            all_jobs.extend(jobs)

            # Check if we've reached the last page
            total = None
            if isinstance(data, dict):
                total = data.get("total", data.get("totalCount", data.get("count")))
                # Also check pagination metadata
                pagination = data.get("pagination", {})
                if isinstance(pagination, dict):
                    total = pagination.get("total", total)

            if total is not None and len(all_jobs) >= int(total):
                logger.debug(f"Reached total ({total}), stopping")
                break

            page += 1

        logger.info(f"Discovered {len(all_jobs)} jobs via Jibe API")
        return all_jobs

    def _discover_jobs_icims_raw(self, keyword: Optional[str] = None) -> list[Job]:
        """Fetch all jobs by scraping raw iCIMS HTML pages."""
        all_jobs = []
        page = 1

        while page <= MAX_PAGES:
            logger.debug(f"Fetching iCIMS page {page}...")
            try:
                html = self._fetch_icims_search_page(page=page, keyword=keyword)
            except Exception as e:
                logger.error(f"Failed to fetch iCIMS page {page}: {e}")
                break

            jobs = self._parse_icims_search_page(html)
            if not jobs:
                break

            all_jobs.extend(jobs)
            page += 1

        logger.info(f"Discovered {len(all_jobs)} jobs via raw iCIMS scraping")
        return all_jobs

    def scrape_job_detail(self, job: Job) -> Job:
        """Fetch full details for a single job posting."""
        if self._api_mode == "jibe" and job.raw_data:
            # Jibe API already returns full details
            return job

        # For raw iCIMS, fetch the detail page
        return self._fetch_icims_job_detail(job)

    def scrape_all(
        self,
        keyword: Optional[str] = None,
        fetch_details: bool = True,
        max_detail_jobs: int = 0,
        today_only: bool = False,
    ) -> list[Job]:
        """
        Override base scrape_all since Jibe mode doesn't need individual detail fetches.
        """
        logger.info(f"[{self.ATS_NAME}] Scraping {self.company.name} ({self.company.portal_url})")

        # Detect API mode
        if self._api_mode is None:
            self._detect_api_mode()

        # Discover jobs
        jobs = self.discover_jobs(keyword=keyword)
        logger.info(f"[{self.ATS_NAME}] Found {len(jobs)} job listings")

        # For Jibe mode, listings already have dates so we can filter early
        # For raw iCIMS mode, listings lack dates — filter AFTER detail fetch
        if today_only and self._api_mode == "jibe":
            jobs = self._filter_recent_jobs(jobs)
            logger.info(f"[{self.ATS_NAME}] Filtered to {len(jobs)} recent jobs (today/yesterday)")

        if self._api_mode != "jibe" and fetch_details:
            # Raw iCIMS needs detail page fetches.
            # Strategy: sample a small batch first to check if this portal
            # exposes posted dates.  If it does, fetch in batches and stop
            # once we pass the recency window.  If not, cap total detail
            # fetches to avoid CI timeouts on huge portals.
            SAMPLE_SIZE = 10
            NO_DATE_CAP = 200  # max details when portal lacks dates

            if max_detail_jobs > 0:
                jobs_to_detail = jobs[:max_detail_jobs]
                logger.info(f"[{self.ATS_NAME}] Fetching details for first {max_detail_jobs} jobs (of {len(jobs)})")
                jobs = self._fetch_details_concurrent(jobs_to_detail)
            elif today_only and len(jobs) > SAMPLE_SIZE:
                # Sample first batch to probe for dates
                sample = self._fetch_details_concurrent(jobs[:SAMPLE_SIZE])
                has_dates = any(j.posted_date for j in sample)

                if has_dates:
                    # Portal has dates — fetch in batches, stop when jobs are old
                    logger.info(f"[{self.ATS_NAME}] Portal exposes dates, fetching details with early stop")
                    all_detailed = list(sample)
                    BATCH = 50
                    consecutive_old = 0
                    offset = SAMPLE_SIZE
                    while offset < len(jobs) and consecutive_old < 2:
                        batch_end = min(offset + BATCH, len(jobs))
                        batch = self._fetch_details_concurrent(jobs[offset:batch_end])
                        all_detailed.extend(batch)
                        recent = [j for j in batch if j.posted_date and self._is_recent(j)]
                        if recent:
                            consecutive_old = 0
                        else:
                            consecutive_old += 1
                            logger.info(f"[{self.ATS_NAME}] No recent jobs in batch at offset {offset}")
                        offset = batch_end
                    jobs = all_detailed
                else:
                    # No dates — cap fetches to avoid timeout, iCIMS lists newest first
                    cap = min(len(jobs), NO_DATE_CAP)
                    logger.info(f"[{self.ATS_NAME}] No dates on detail pages, capping at {cap} jobs (of {len(jobs)})")
                    remaining = self._fetch_details_concurrent(jobs[SAMPLE_SIZE:cap])
                    jobs = list(sample) + remaining
            else:
                jobs = self._fetch_details_concurrent(jobs)

        # For raw iCIMS, filter AFTER detail fetch (now we have dates)
        if today_only and self._api_mode != "jibe":
            has_any_dates = any(j.posted_date for j in jobs)
            if has_any_dates:
                jobs = self._filter_recent_jobs(jobs)
                logger.info(f"[{self.ATS_NAME}] Filtered to {len(jobs)} recent jobs (last 2 days)")
            else:
                logger.info(f"[{self.ATS_NAME}] No posted dates on detail pages, returning all {len(jobs)} jobs")

        logger.info(f"[{self.ATS_NAME}] Scraped {len(jobs)} jobs total")

        self.company.job_count = len(jobs)
        self.company.verified = True

        return jobs

    @staticmethod
    def _is_recent(job: Job) -> bool:
        """Check if a job was posted within the last 2 days."""
        from datetime import date, timedelta
        if not job.posted_date:
            return False
        job_date = job.posted_date.date() if hasattr(job.posted_date, 'date') else job.posted_date
        return job_date >= date.today() - timedelta(days=2)

    # ──────────────────────────────────────────────
    # Utilities
    # ──────────────────────────────────────────────

    @staticmethod
    def _strip_html(html: str) -> str:
        """Convert HTML to clean text."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        # Preserve newlines from block elements
        for br in soup.find_all(["br", "p", "div", "li"]):
            br.insert_before("\n")
        text = soup.get_text(separator=" ", strip=True)
        # Clean up excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()
