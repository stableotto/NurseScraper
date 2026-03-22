"""Abstract base scraper with rate limiting, retries, and User-Agent rotation."""

from __future__ import annotations

import abc
import logging
import random
import time
from typing import Optional

import requests
from fake_useragent import UserAgent
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from models.job import Job
from models.company import Company

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    """Base class all ATS scrapers must extend."""

    ATS_NAME: str = "base"  # Override in subclass

    def __init__(
        self,
        company: Company,
        *,
        rate_limit: float = 1.5,  # seconds between requests
        max_retries: int = 3,
        timeout: int = 30,
    ):
        self.company = company
        self.rate_limit = rate_limit
        self.max_retries = max_retries
        self.timeout = timeout
        self._session = self._build_session()
        self._last_request_time = 0.0
        self._ua = UserAgent(fallback="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")

    def _build_session(self) -> requests.Session:
        """Create a requests session with default headers."""
        session = requests.Session()
        session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        )
        return session

    def _rotate_user_agent(self) -> None:
        """Rotate the User-Agent header to avoid detection."""
        self._session.headers["User-Agent"] = self._ua.random

    def _throttle(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        jitter = random.uniform(0, 0.5)
        wait_time = self.rate_limit + jitter - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying request (attempt {retry_state.attempt_number})..."
        ),
    )
    def _get(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited GET request with retries and UA rotation."""
        self._throttle()
        self._rotate_user_agent()
        kwargs.setdefault("timeout", self.timeout)
        response = self._session.get(url, **kwargs)
        response.raise_for_status()
        return response

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    )
    def _post(self, url: str, **kwargs) -> requests.Response:
        """Rate-limited POST request with retries and UA rotation."""
        self._throttle()
        self._rotate_user_agent()
        kwargs.setdefault("timeout", self.timeout)
        response = self._session.post(url, **kwargs)
        response.raise_for_status()
        return response

    # --- Abstract interface ---

    @abc.abstractmethod
    def discover_jobs(self, keyword: Optional[str] = None, **kwargs) -> list[Job]:
        """
        Fetch all job listings from this company's portal.

        Args:
            keyword: Optional keyword filter (e.g., "nurse", "RN")
            **kwargs: Additional options (e.g., today_only for early termination)

        Returns:
            List of Job objects
        """
        ...

    @abc.abstractmethod
    def scrape_job_detail(self, job: Job) -> Job:
        """
        Fetch full details for a single job posting.

        Args:
            job: A Job with at minimum an id and url populated

        Returns:
            The same Job object enriched with full details
        """
        ...

    def _filter_recent_jobs(self, jobs: list[Job]) -> list[Job]:
        """
        Filter jobs to only those posted today or yesterday.

        Uses listing data (raw_data) before details are fetched.
        This enables filtering BEFORE the expensive detail fetch.
        """
        from datetime import date, timedelta

        today = date.today()
        yesterday = today - timedelta(days=1)
        filtered = []

        for job in jobs:
            # Check posted_date if already parsed
            if job.posted_date:
                job_date = job.posted_date.date() if hasattr(job.posted_date, 'date') else job.posted_date
                if job_date >= yesterday:
                    filtered.append(job)
                    continue

            # Check raw listing data for posted text
            raw = job.raw_data or {}
            posted_on = ""

            # Workday: listing.posted_on
            if "listing" in raw:
                posted_on = raw["listing"].get("posted_on", "")
            # iCIMS: posted_date or postedOn
            elif "posted_date" in raw:
                posted_on = raw.get("posted_date", "")
            elif "postedOn" in raw:
                posted_on = raw.get("postedOn", "")

            posted_lower = posted_on.lower() if posted_on else ""

            # Match "Posted Today", "Posted Yesterday", "Posted 1 Day Ago"
            if any(term in posted_lower for term in ["today", "yesterday", "1 day", "just posted"]):
                filtered.append(job)

        return filtered

    def scrape_all(
        self,
        keyword: Optional[str] = None,
        fetch_details: bool = True,
        max_detail_jobs: int = 0,
        today_only: bool = False,
    ) -> list[Job]:
        """
        Full scrape workflow: discover jobs and optionally fetch details.

        Args:
            keyword: Optional keyword filter for the search
            fetch_details: If True, fetch full details for each job (slow).
                          If False, return only listing info (fast).
            max_detail_jobs: Max jobs to fetch details for. 0 = no limit.
                            Only applies when fetch_details=True.
            today_only: If True, filter to jobs posted today/yesterday BEFORE
                       fetching details (much faster for large portals).

        Returns:
            List of Job objects
        """
        logger.info(f"[{self.ATS_NAME}] Scraping {self.company.name} ({self.company.portal_url})")

        # Step 1: Discover all job listings (pass today_only for early termination)
        jobs = self.discover_jobs(keyword=keyword, today_only=today_only)
        logger.info(f"[{self.ATS_NAME}] Found {len(jobs)} job listings")

        # Step 2: Try to filter to today's jobs BEFORE fetching details (huge speedup)
        # If filter drops ALL jobs but we had some, listings likely lack date info —
        # defer filtering to after detail fetch instead
        filter_after_details = False
        if today_only:
            filtered = self._filter_recent_jobs(jobs)
            if filtered or not jobs:
                jobs = filtered
                logger.info(f"[{self.ATS_NAME}] Filtered to {len(jobs)} recent jobs (today/yesterday)")
            else:
                # Listings lack date info; will filter after detail fetch
                filter_after_details = True
                logger.info(f"[{self.ATS_NAME}] Listings lack dates, deferring filter to after detail fetch")

        # Step 3: Optionally fetch full details for each job
        if not fetch_details:
            logger.info(f"[{self.ATS_NAME}] Skipping detail fetch (--skip-details)")
            detailed_jobs = jobs
        else:
            # Optionally limit detail fetches
            if max_detail_jobs > 0 and len(jobs) > max_detail_jobs:
                jobs_to_detail = jobs[:max_detail_jobs]
                jobs_listing_only = jobs[max_detail_jobs:]
                logger.info(f"[{self.ATS_NAME}] Fetching details for first {max_detail_jobs} jobs (of {len(jobs)})")
            else:
                jobs_to_detail = jobs
                jobs_listing_only = []

            detailed_jobs = []
            for i, job in enumerate(jobs_to_detail):
                try:
                    enriched = self.scrape_job_detail(job)
                    detailed_jobs.append(enriched)

                    if (i + 1) % 25 == 0:
                        logger.info(f"[{self.ATS_NAME}] Processed {i + 1}/{len(jobs_to_detail)} jobs")
                except Exception as e:
                    logger.error(f"[{self.ATS_NAME}] Failed to scrape job {job.id}: {e}")
                    continue

            # Add remaining jobs without details
            detailed_jobs.extend(jobs_listing_only)

        # Step 4: Filter after detail fetch if we deferred earlier
        if filter_after_details:
            detailed_jobs = self._filter_recent_jobs(detailed_jobs)
            logger.info(f"[{self.ATS_NAME}] Filtered to {len(detailed_jobs)} recent jobs (today/yesterday)")

        logger.info(f"[{self.ATS_NAME}] Scraped {len(detailed_jobs)} jobs total")

        # Update company metadata
        self.company.job_count = len(detailed_jobs)
        self.company.verified = True

        return detailed_jobs
