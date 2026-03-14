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
    def discover_jobs(self, keyword: Optional[str] = None) -> list[Job]:
        """
        Fetch all job listings from this company's portal.

        Args:
            keyword: Optional keyword filter (e.g., "nurse", "RN")

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

    def scrape_all(self, keyword: Optional[str] = None, category_filter: Optional[str] = None) -> list[Job]:
        """
        Full scrape workflow: discover jobs, fetch details, classify.

        Args:
            keyword: Optional keyword filter for the search
            category_filter: Optional category to filter by (e.g., "nursing", "pharmacy")

        Returns:
            List of fully populated Job objects
        """
        logger.info(f"[{self.ATS_NAME}] Scraping {self.company.name} ({self.company.portal_url})")

        # Step 1: Discover all job listings
        jobs = self.discover_jobs(keyword=keyword)
        logger.info(f"[{self.ATS_NAME}] Found {len(jobs)} job listings")

        # Step 2: Fetch full details for each job and classify
        detailed_jobs = []
        for i, job in enumerate(jobs):
            try:
                enriched = self.scrape_job_detail(job)
                enriched.classify()  # Multi-category classification
                detailed_jobs.append(enriched)

                if (i + 1) % 25 == 0:
                    logger.info(f"[{self.ATS_NAME}] Processed {i + 1}/{len(jobs)} jobs")
            except Exception as e:
                logger.error(f"[{self.ATS_NAME}] Failed to scrape job {job.id}: {e}")
                continue

        # Log category breakdown
        from collections import Counter
        all_categories = [cat for job in detailed_jobs for cat in job.categories]
        category_counts = Counter(all_categories)
        if category_counts:
            logger.info(f"[{self.ATS_NAME}] Categories: {dict(category_counts)}")

        # Step 3: Filter by category if requested
        if category_filter:
            detailed_jobs = [j for j in detailed_jobs if category_filter in j.categories]
            logger.info(f"[{self.ATS_NAME}] {len(detailed_jobs)} jobs after {category_filter} filter")

        # Update company metadata
        self.company.job_count = len(detailed_jobs)
        self.company.verified = True

        return detailed_jobs
