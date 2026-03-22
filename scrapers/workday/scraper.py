"""
Workday Job Scraper

Scrapes job listings from Workday-powered career portals.
Uses the internal CXS (Candidate Experience Services) JSON API.

API endpoints:
  - List jobs: POST /wday/cxs/{tenant}/{site}/jobs
  - Job detail: GET /wday/cxs/{tenant}/{site}/job/{externalPath}
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from dateutil.parser import parse as parse_date

from models.job import Job
from models.company import Company
from scrapers.base import BaseScraper
from scrapers.workday.config import (
    DEFAULT_PAGE_SIZE,
    MAX_JOBS,
)

logger = logging.getLogger(__name__)


class WorkdayScraper(BaseScraper):
    """Scraper for Workday career portals using the CXS API."""

    ATS_NAME = "workday"

    def __init__(self, company: Company, **kwargs):
        super().__init__(company, **kwargs)
        self._tenant: str = ""
        self._wd_instance: str = ""
        self._site: str = ""
        self._base_url: str = ""
        self._parse_portal_url()

    def _parse_portal_url(self) -> None:
        """
        Parse the portal URL to extract tenant, instance, and site.

        Example URL: https://rch.wd108.myworkdayjobs.com/Careers
        - tenant: rch
        - wd_instance: wd108
        - site: Careers
        """
        url = self.company.portal_url.rstrip("/")
        parsed = urlparse(url)

        # Extract tenant and wd instance from hostname
        # Format: {tenant}.{wd_instance}.myworkdayjobs.com
        hostname = parsed.hostname or ""
        parts = hostname.split(".")

        if len(parts) >= 3 and "myworkdayjobs" in hostname:
            self._tenant = parts[0]
            self._wd_instance = parts[1]
        else:
            raise ValueError(f"Invalid Workday URL format: {url}")

        # Extract site from path (e.g., /Careers or /en-US/Careers)
        path_parts = [p for p in parsed.path.split("/") if p]
        if path_parts:
            # Site is typically the last meaningful path segment
            # Handle locale prefixes like /en-US/
            self._site = path_parts[-1]
        else:
            self._site = "External"  # Common default

        self._base_url = f"https://{self._tenant}.{self._wd_instance}.myworkdayjobs.com"

        logger.debug(
            f"Parsed Workday URL: tenant={self._tenant}, "
            f"instance={self._wd_instance}, site={self._site}"
        )

    def _build_api_url(self, endpoint: str = "jobs") -> str:
        """Build the CXS API URL."""
        return f"{self._base_url}/wday/cxs/{self._tenant}/{self._site}/{endpoint}"

    # ──────────────────────────────────────────────
    # Job Listing API
    # ──────────────────────────────────────────────

    def _fetch_jobs_page(
        self,
        offset: int = 0,
        limit: int = DEFAULT_PAGE_SIZE,
        search_text: str = "",
        facets: Optional[dict] = None,
    ) -> dict:
        """Fetch a page of jobs from the Workday CXS API."""
        url = self._build_api_url("jobs")

        payload = {
            "appliedFacets": facets or {},
            "limit": limit,
            "offset": offset,
            "searchText": search_text,
        }

        resp = self._post(url, json=payload, headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        return resp.json()

    def _parse_job_listing(self, item: dict) -> dict:
        """Parse a job listing item from the search results."""
        return {
            "title": item.get("title", ""),
            "external_path": item.get("externalPath", ""),
            "location": item.get("locationsText", ""),
            "posted_on": item.get("postedOn", ""),
            "bullet_fields": item.get("bulletFields", []),
        }

    # ──────────────────────────────────────────────
    # Job Detail API
    # ──────────────────────────────────────────────

    def _fetch_job_detail(self, external_path: str) -> dict:
        """Fetch full job details from the CXS API."""
        # external_path already includes leading slash
        # e.g., "/job/Main-Campus---Orange/Registered-Nurse_R-19782"
        url = f"{self._base_url}/wday/cxs/{self._tenant}/{self._site}{external_path}"

        resp = self._get(url, headers={"Accept": "application/json"})
        return resp.json()

    def _parse_job_detail(self, data: dict, listing: dict) -> Job:
        """Parse full job details into a Job object."""
        info = data.get("jobPostingInfo", {})
        org = data.get("hiringOrganization", {})

        # Extract job ID from requisition ID or posting ID
        job_id = info.get("jobReqId", "") or info.get("jobPostingId", "")

        # Title
        title = info.get("title", listing.get("title", ""))

        # Location - combine primary and additional locations
        location = info.get("location", "")
        additional = info.get("additionalLocations", [])
        if additional:
            location = f"{location}; {'; '.join(additional)}" if location else "; ".join(additional)

        # Time type (Full time, Part time)
        time_type = info.get("timeType", "")

        # Posted date
        posted_date = None
        start_date = info.get("startDate", "")
        if start_date:
            try:
                posted_date = parse_date(start_date)
            except (ValueError, TypeError):
                pass

        # URL
        url = info.get("externalUrl", "")
        if not url:
            url = f"{self._base_url}/{self._site}{listing.get('external_path', '')}"

        # Description - parse HTML
        description_html = info.get("jobDescription", "")
        description = self._strip_html(description_html)

        # Extract salary from description if present
        salary = self._extract_salary(description_html)

        # Extract qualifications (often embedded in description)
        qualifications = self._extract_qualifications(description)

        # Company name from hiring organization or portal
        company_name = org.get("name", "") or self.company.name

        return Job(
            id=job_id,
            source_ats="workday",
            company_name=company_name,
            title=title,
            department="",  # Workday doesn't expose this directly
            location=location,
            job_type=time_type,
            posted_date=posted_date,
            url=url,
            description=description,
            qualifications=qualifications,
            salary_range=salary,
            raw_data=data,
        )

    # ──────────────────────────────────────────────
    # Public Interface (implements BaseScraper)
    # ──────────────────────────────────────────────

    def discover_jobs(self, keyword: Optional[str] = None) -> list[Job]:
        """Discover all job listings from this portal."""
        all_listings = []
        offset = 0
        total = None

        while True:
            logger.debug(f"Fetching Workday jobs offset={offset}...")

            try:
                data = self._fetch_jobs_page(
                    offset=offset,
                    limit=DEFAULT_PAGE_SIZE,
                    search_text=keyword or "",
                )
            except Exception as e:
                logger.error(f"Failed to fetch Workday page at offset {offset}: {e}")
                break

            # Get total count on first request
            if total is None:
                total = data.get("total", 0)
                logger.info(f"Workday reports {total} total jobs")

            # Parse job listings
            job_postings = data.get("jobPostings", [])
            if not job_postings:
                logger.debug("No more jobs, stopping pagination")
                break

            for item in job_postings:
                listing = self._parse_job_listing(item)
                all_listings.append(listing)

            offset += len(job_postings)

            # Safety limits
            if offset >= total:
                break
            if len(all_listings) >= MAX_JOBS:
                logger.warning(f"Hit safety limit of {MAX_JOBS} jobs")
                break

        logger.info(f"Discovered {len(all_listings)} job listings from Workday")

        # Convert listings to partial Job objects (will be enriched in scrape_job_detail)
        jobs = []
        for listing in all_listings:
            # Create minimal job with external_path stored for detail fetch
            job = Job(
                id=listing.get("bullet_fields", [""])[0] or "",  # Req ID often in bullet_fields
                source_ats="workday",
                company_name=self.company.name,
                title=listing["title"],
                location=listing["location"],
                url=f"{self._base_url}/{self._site}{listing['external_path']}",
                raw_data={"external_path": listing["external_path"], "listing": listing},
            )
            jobs.append(job)

        return jobs

    def scrape_job_detail(self, job: Job) -> Job:
        """Fetch full details for a single job posting."""
        if not job.raw_data or "external_path" not in job.raw_data:
            logger.warning(f"Job {job.id} missing external_path, skipping detail fetch")
            return job

        external_path = job.raw_data["external_path"]
        listing = job.raw_data.get("listing", {})

        try:
            data = self._fetch_job_detail(external_path)
            enriched = self._parse_job_detail(data, listing)
            return enriched
        except Exception as e:
            logger.error(f"Failed to fetch Workday job detail for {job.id}: {e}")
            return job

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

    @staticmethod
    def _extract_salary(html: str) -> Optional[str]:
        """Extract salary range from job description HTML."""
        if not html:
            return None

        # Common patterns in Workday job descriptions:
        # "Minimum $56.96 Midpoint $74.05 Maximum $91.14"
        # "Pay Range: $50,000 - $70,000"
        # "$25.00 - $35.00 per hour"

        patterns = [
            # Minimum/Midpoint/Maximum pattern
            r"Minimum\s*\$?([\d,\.]+)\s*(?:Midpoint\s*\$?([\d,\.]+))?\s*Maximum\s*\$?([\d,\.]+)",
            # Range pattern with dash
            r"(?:Pay\s*Range|Salary|Compensation)[:\s]*\$?([\d,\.]+)\s*[-–to]+\s*\$?([\d,\.]+)",
            # Hourly rate pattern
            r"\$?([\d,\.]+)\s*[-–to]+\s*\$?([\d,\.]+)\s*(?:per\s*hour|/hr|hourly)",
            # Annual salary pattern
            r"\$?([\d,\.]+)\s*[-–to]+\s*\$?([\d,\.]+)\s*(?:per\s*year|/yr|annually)",
        ]

        text = html.replace(",", "")  # Remove commas for easier parsing

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = [g for g in match.groups() if g]
                if len(groups) >= 2:
                    return f"${groups[0]} - ${groups[-1]}"
                elif len(groups) == 1:
                    return f"${groups[0]}"

        return None

    @staticmethod
    def _extract_qualifications(description: str) -> str:
        """Extract qualifications section from description text."""
        if not description:
            return ""

        # Look for common qualification headers
        patterns = [
            r"(?:Qualifications?|Requirements?|What You['']?ll Need)[:\s]*\n([\s\S]*?)(?:\n\n|\Z)",
            r"(?:Required|Minimum)\s*(?:Qualifications?|Requirements?)[:\s]*\n([\s\S]*?)(?:\n\n|\Z)",
        ]

        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1).strip()[:1000]  # Limit length

        return ""
