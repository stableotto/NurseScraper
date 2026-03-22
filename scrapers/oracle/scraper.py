"""Oracle HCM Recruiting Cloud scraper.

Oracle HCM provides a clean REST API:
- Endpoint: /hcmRestApi/resources/latest/recruitingCEJobRequisitions
- Finder: findReqs;siteNumber={site},limit={n},sortBy=POSTING_DATES_DESC
- Returns JSON with requisitionList array

Example URL patterns:
- https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs
- https://company.fa.us6.oraclecloud.com/hcmUI/CandidateExperience/en/sites/Careers/requisitions
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from models.job import Job
from models.company import Company
from scrapers.base import BaseScraper
from scrapers.oracle.config import DEFAULT_PAGE_SIZE, MAX_JOBS

logger = logging.getLogger(__name__)


class OracleScraper(BaseScraper):
    """Scraper for Oracle HCM Recruiting Cloud career sites."""

    ATS_NAME = "oracle"

    def __init__(self, company: Company, **kwargs):
        """Initialize Oracle HCM scraper.

        Args:
            company: Company object with portal_url set to Oracle HCM career site
        """
        super().__init__(company, **kwargs)

        url = company.portal_url
        parsed = urlparse(url)

        # Build API base URL
        self._api_base = f"https://{parsed.hostname}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"

        # Extract site number from URL: /sites/{site}/jobs
        self._site_number = "CX_1"  # default
        site_match = re.search(r'/sites/([^/]+)/', url)
        if site_match:
            self._site_number = site_match.group(1)

    def _build_api_url(
        self,
        offset: int = 0,
        limit: int = DEFAULT_PAGE_SIZE,
        keyword: Optional[str] = None,
    ) -> str:
        """Build API URL with finder parameters."""
        finder_params = [
            f"siteNumber={self._site_number}",
            "facetsList=LOCATIONS;WORK_LOCATIONS;WORKPLACE_TYPES;TITLES;CATEGORIES;ORGANIZATIONS;POSTING_DATES;FLEX_FIELDS",
            f"limit={limit}",
            f"offset={offset}",
            "sortBy=POSTING_DATES_DESC",
        ]

        if keyword:
            finder_params.insert(0, f"keyword={keyword}")

        finder = ";".join(finder_params)

        return f"{self._api_base}?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values&finder=findReqs;{finder}"

    def _get_job_url(self, job_id: str) -> str:
        """Build public job detail URL."""
        parsed = urlparse(self.company.portal_url)
        return f"https://{parsed.hostname}/hcmUI/CandidateExperience/en/sites/{self._site_number}/job/{job_id}"

    # ──────────────────────────────────────────────
    # Job Discovery
    # ──────────────────────────────────────────────

    def discover_jobs(self, keyword: Optional[str] = None, **kwargs) -> list[Job]:
        """Discover all job listings from the Oracle HCM portal.

        Args:
            keyword: Optional search keyword

        Returns:
            List of Job objects
        """
        jobs = []
        offset = 0
        has_more = True

        while has_more and len(jobs) < MAX_JOBS:
            url = self._build_api_url(offset=offset, keyword=keyword)

            try:
                response = self._get(url)
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to fetch jobs at offset {offset}: {e}")
                break

            # Get results from the nested structure
            items = data.get("items", [])
            if not items:
                break

            # First item contains the search results
            search_result = items[0] if items else {}
            requisitions = search_result.get("requisitionList", [])
            total_count = search_result.get("TotalJobsCount", 0)

            if offset == 0:
                logger.info(f"Oracle HCM reports {total_count} total jobs")

            for req in requisitions:
                try:
                    job = self._parse_requisition(req)
                    if job:
                        jobs.append(job)
                except Exception as e:
                    logger.warning(f"Failed to parse requisition: {e}")

            # Check pagination
            has_more = data.get("hasMore", False) or len(requisitions) == DEFAULT_PAGE_SIZE
            offset += len(requisitions)

            if len(requisitions) < DEFAULT_PAGE_SIZE:
                has_more = False

            logger.info(f"Discovered {len(jobs)} jobs so far (offset: {offset})")

        logger.info(f"Discovered {len(jobs)} job listings from Oracle HCM")
        return jobs

    def _parse_requisition(self, req: dict) -> Optional[Job]:
        """Parse a job requisition from API response."""
        job_id = req.get("Id")
        if not job_id:
            return None

        title = req.get("Title", "")
        posted_date_str = req.get("PostedDate")  # Format: "2026-03-13"
        workplace_type = req.get("WorkplaceType", "")

        # Parse location from PrimaryLocation or GeographyNodePath
        city = ""
        state = ""
        primary_location = req.get("PrimaryLocation", "")
        if primary_location:
            # Format varies: "City, State" or "City, State, Country"
            parts = [p.strip() for p in primary_location.split(",")]
            if len(parts) >= 2:
                city = parts[0]
                state = parts[1]
            elif len(parts) == 1:
                city = parts[0]

        # Parse posted date
        posted_date = None
        if posted_date_str:
            try:
                posted_date = datetime.strptime(posted_date_str, "%Y-%m-%d")
            except ValueError:
                pass

        # Get category from CategoryName or JobFamily
        category = req.get("CategoryName", "") or req.get("JobFamilyName", "")

        # Get organization/department
        department = req.get("OrganizationName", "")

        # Build location string
        location = f"{city}, {state}" if city and state else city or state or ""

        job = Job(
            id=str(job_id),
            source_ats=self.ATS_NAME,
            company_name=self.company.name,
            title=title,
            department=department,
            location=location,
            posted_date=posted_date,
            url=self._get_job_url(job_id),
            raw_data={"category": category, "workplace_type": workplace_type},
        )

        return job

    # ──────────────────────────────────────────────
    # Job Details
    # ──────────────────────────────────────────────

    def scrape_job_detail(self, job: Job) -> Job:
        """Fetch full details for a single job posting.

        Oracle HCM provides most details in the list response,
        but we can fetch additional info if needed.

        Args:
            job: Job object with external_id set

        Returns:
            Job object with full details
        """
        if not job.external_id:
            return job

        # Build detail API URL
        detail_url = f"{self._api_base}/{job.external_id}"

        try:
            response = self._get(detail_url)
            data = response.json()
        except Exception as e:
            logger.warning(f"Failed to fetch job detail {job.external_id}: {e}")
            return job

        # Extract description from ExternalDescriptionStr
        description = data.get("ExternalDescriptionStr", "")
        if description:
            job.description = description

        # Extract qualifications
        qualifications = data.get("QualificationsStr", "")
        if qualifications:
            job.qualifications = qualifications

        # Extract responsibilities
        responsibilities = data.get("ResponsibilitiesStr", "")
        if responsibilities:
            job.responsibilities = responsibilities

        return job

    # ──────────────────────────────────────────────
    # Main Scrape
    # ──────────────────────────────────────────────

    def scrape_all(
        self,
        keyword: Optional[str] = None,
        fetch_details: bool = False,
        max_detail_jobs: int = 0,
        today_only: bool = False,
    ) -> list[Job]:
        """Full scrape: discover jobs and optionally fetch details.

        Args:
            keyword: Optional search keyword
            fetch_details: Whether to fetch full job details (slower)
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
