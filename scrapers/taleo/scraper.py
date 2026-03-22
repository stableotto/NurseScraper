"""Taleo career site scraper.

Taleo embeds job data in JavaScript fillList() calls:
- Job list: fillList('requisitionListInterface', 'listRequisition', [...])
- Job detail: fillList('requisitionDescriptionInterface', 'descRequisition', [...])

Data is a flat array with ~44 fields per job (list) or ~56 fields (detail).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

from models.job import Job
from models.company import Company
from scrapers.base import BaseScraper
from scrapers.taleo.config import (
    DEFAULT_PAGE_SIZE,
    DETAIL_FIELDS,
    LIST_FIELDS,
    MAX_JOBS,
    MAX_PAGES,
)

logger = logging.getLogger(__name__)


class TaleoScraper(BaseScraper):
    """Scraper for Taleo career sites."""

    ATS_NAME = "taleo"

    def __init__(self, company: Company, **kwargs):
        """Initialize Taleo scraper.

        Args:
            company: Company object with portal_url set to Taleo career site
        """
        super().__init__(company, **kwargs)

        # Parse Taleo URL to extract base URL and career section
        url = company.portal_url
        parsed = urlparse(url)
        self._base_url = f"{parsed.scheme}://{parsed.netloc}"

        # Extract career section from path: /careersection/{section}/joblist.ftl
        self._career_section = "jobsearch"  # default
        if "/careersection/" in parsed.path:
            parts = parsed.path.split("/careersection/")
            if len(parts) > 1:
                section_part = parts[1].split("/")[0]
                if section_part:
                    self._career_section = section_part

    def _get_list_url(self) -> str:
        """Build job list URL."""
        return f"{self._base_url}/careersection/{self._career_section}/joblist.ftl"

    def _get_detail_url(self, job_id: str) -> str:
        """Build job detail URL."""
        return f"{self._base_url}/careersection/{self._career_section}/jobdetail.ftl?job={job_id}"

    def _parse_filllist(self, html: str, interface: str, list_name: str) -> list[list[str]]:
        """Parse fillList() call and extract job data arrays.

        Args:
            html: Page HTML content
            interface: Interface name (e.g., 'requisitionListInterface')
            list_name: List name (e.g., 'listRequisition')

        Returns:
            List of job data arrays
        """
        # Pattern: fillList('interface', 'listName', [...])
        pattern = rf"fillList\('{interface}',\s*'{list_name}',\s*\[(.*?)\]\)"
        match = re.search(pattern, html, re.DOTALL)

        if not match:
            logger.warning(f"No fillList found for {interface}.{list_name}")
            return []

        content = match.group(1)

        # Parse values between quotes
        values = re.findall(r"'((?:[^'\\]|\\.)*)'", content)

        if not values:
            return []

        # Each job has 44 fields in the list
        fields_per_job = 44

        # Split into job arrays
        jobs = []
        current_job = []

        for val in values:
            current_job.append(val)
            if len(current_job) == fields_per_job:
                jobs.append(current_job)
                current_job = []

        # Handle partial last job
        if current_job and len(current_job) > 10:
            jobs.append(current_job)

        return jobs

    def _decode_html_content(self, encoded: str) -> str:
        """Decode Taleo's URL-encoded HTML content.

        Taleo prefixes encoded content with '!*!' marker.
        """
        if not encoded:
            return ""

        # Remove !*! prefix if present
        if encoded.startswith("!*!"):
            encoded = encoded[3:]

        # URL decode
        html = unquote(encoded)

        # Strip HTML tags and normalize whitespace
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ")
        text = " ".join(text.split())

        return text

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse Taleo date formats."""
        if not date_str:
            return None

        # Try various formats
        formats = [
            "%b %d, %Y",  # Mar 12, 2026
            "%b %d, %Y, %I:%M:%S %p",  # Mar 12, 2026, 6:03:20 PM
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        return None

    # ──────────────────────────────────────────────
    # Job Discovery
    # ──────────────────────────────────────────────

    def discover_jobs(self, keyword: Optional[str] = None) -> list[Job]:
        """Discover all job listings from the Taleo portal.

        Args:
            keyword: Optional search keyword (not fully supported yet)

        Returns:
            List of Job objects with basic info
        """
        jobs = []
        url = self._get_list_url()

        try:
            response = self._get(url)
        except Exception as e:
            logger.error(f"Failed to fetch job list: {e}")
            return jobs

        # Parse total jobs from page
        total_match = re.search(
            r'listRequisition\.nbElements["\s]*value="(\d+)"',
            response.text
        )
        if total_match:
            total_jobs = int(total_match.group(1))
            logger.info(f"Taleo reports {total_jobs} total jobs")

        # Parse job data from fillList
        job_arrays = self._parse_filllist(
            response.text,
            "requisitionListInterface",
            "listRequisition"
        )

        logger.info(f"Discovered {len(job_arrays)} job listings from Taleo")

        for job_data in job_arrays[:MAX_JOBS]:
            try:
                job = self._parse_list_job(job_data)
                if job:
                    jobs.append(job)
            except Exception as e:
                logger.warning(f"Failed to parse job: {e}")

        return jobs

    def _parse_list_job(self, data: list[str]) -> Optional[Job]:
        """Parse a job from the list fillList data."""
        if len(data) < 23:
            return None

        job_id = data[LIST_FIELDS["job_id"]] if len(data) > LIST_FIELDS["job_id"] else None
        if not job_id:
            return None

        title = data[LIST_FIELDS["title"]] if len(data) > LIST_FIELDS["title"] else ""
        requisition = data[LIST_FIELDS["requisition_number"]] if len(data) > LIST_FIELDS["requisition_number"] else ""
        location = data[LIST_FIELDS["location"]] if len(data) > LIST_FIELDS["location"] else ""
        category = data[LIST_FIELDS["category"]] if len(data) > LIST_FIELDS["category"] else ""
        job_type = data[LIST_FIELDS["job_type"]] if len(data) > LIST_FIELDS["job_type"] else ""
        posted_str = data[LIST_FIELDS["posted_date"]] if len(data) > LIST_FIELDS["posted_date"] else ""
        department = data[LIST_FIELDS["department"]] if len(data) > LIST_FIELDS["department"] else ""

        # Parse location
        city, state = "", ""
        if location:
            # Format: "United States-California-Long Beach"
            parts = location.split("-")
            if len(parts) >= 3:
                state = parts[1]
                city = parts[2]
            elif len(parts) == 2:
                state = parts[0]
                city = parts[1]

        # Parse date
        posted_date = self._parse_date(posted_str)

        # Build location string
        location = f"{city}, {state}" if city and state else city or state or ""

        job = Job(
            id=requisition or job_id,
            source_ats=self.ATS_NAME,
            company_name=self.company.name,
            title=title,
            department=department,
            location=location,
            job_type=job_type,
            posted_date=posted_date,
            url=self._get_detail_url(job_id),
            raw_data={"category": category, "job_id": job_id},
        )

        return job

    # ──────────────────────────────────────────────
    # Job Details
    # ──────────────────────────────────────────────

    def scrape_job_detail(self, job: Job) -> Job:
        """Fetch full details for a single job posting.

        Args:
            job: Job object with url set

        Returns:
            Job object with full details
        """
        if not job.url:
            return job

        # Extract job ID from URL
        job_id_match = re.search(r"job=(\d+)", job.url)
        if not job_id_match:
            return job

        job_id = job_id_match.group(1)

        try:
            response = self._get(job.url)
        except Exception as e:
            logger.warning(f"Failed to fetch job detail {job_id}: {e}")
            return job

        # Parse detail data from fillList
        pattern = r"fillList\('requisitionDescriptionInterface',\s*'descRequisition',\s*\[(.*?)\]\)"
        match = re.search(pattern, response.text, re.DOTALL)

        if not match:
            logger.warning(f"No detail data found for job {job_id}")
            return job

        # Parse values
        values = re.findall(r"'((?:[^'\\]|\\.)*)'", match.group(1))

        if len(values) > DETAIL_FIELDS["description"]:
            description = self._decode_html_content(values[DETAIL_FIELDS["description"]])
            if description:
                job.description = description

        if len(values) > DETAIL_FIELDS["qualifications"]:
            qualifications = self._decode_html_content(values[DETAIL_FIELDS["qualifications"]])
            if qualifications:
                job.qualifications = qualifications

        if len(values) > DETAIL_FIELDS["specialty"]:
            specialty = values[DETAIL_FIELDS["specialty"]]
            if specialty:
                if job.raw_data:
                    job.raw_data["specialty"] = specialty
                else:
                    job.raw_data = {"specialty": specialty}

        return job

    # ──────────────────────────────────────────────
    # Main Scrape
    # ──────────────────────────────────────────────

    def scrape_all(
        self,
        keyword: Optional[str] = None,
        fetch_details: bool = True,
        max_detail_jobs: int = 0,
        today_only: bool = False,
    ) -> list[Job]:
        """Full scrape: discover jobs and optionally fetch details.

        Args:
            keyword: Optional search keyword
            fetch_details: Whether to fetch full job details
            max_detail_jobs: Max jobs to fetch details for. 0 = no limit.
            today_only: If True, filter to recent jobs before detail fetch.

        Returns:
            List of Job objects
        """
        jobs = self.discover_jobs(keyword=keyword)

        if today_only:
            jobs = self._filter_recent_jobs(jobs)
            logger.info(f"[{self.ATS_NAME}] Filtered to {len(jobs)} recent jobs (today/yesterday)")

        if fetch_details:
            jobs_to_detail = jobs
            if max_detail_jobs > 0 and len(jobs) > max_detail_jobs:
                jobs_to_detail = jobs[:max_detail_jobs]
                logger.info(f"[{self.ATS_NAME}] Fetching details for first {max_detail_jobs} jobs (of {len(jobs)})")

            for i, job in enumerate(jobs_to_detail):
                job = self.scrape_job_detail(job)
                jobs_to_detail[i] = job

                if (i + 1) % 25 == 0:
                    logger.info(f"[{self.ATS_NAME}] Processed {i + 1}/{len(jobs_to_detail)} job details")

            jobs = jobs_to_detail

        return jobs
