"""
TalentBrew/Radancy Job Scraper

Scrapes job listings from TalentBrew-powered career sites.
TalentBrew is a career site platform that wraps various ATS systems
(Taleo, Workday, iCIMS, etc.) with a branded experience.

Data extraction:
  - Job listings: Parse HTML for job links with data-job-id attributes
  - Job details: Extract JSON-LD schema.org JobPosting from detail pages
"""

from __future__ import annotations

import json
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
from scrapers.talentbrew.config import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGES,
    MAX_JOBS,
    TALENTBREW_INDICATORS,
)

logger = logging.getLogger(__name__)


class TalentBrewScraper(BaseScraper):
    """Scraper for TalentBrew/Radancy career sites."""

    ATS_NAME = "talentbrew"

    def __init__(self, company: Company, **kwargs):
        super().__init__(company, **kwargs)
        self._base_url = self._normalize_base_url(company.portal_url)
        self._is_talentbrew: Optional[bool] = None

    def _normalize_base_url(self, url: str) -> str:
        """Normalize the portal URL to base domain."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _is_talentbrew_site(self, html: str) -> bool:
        """Check if the site is powered by TalentBrew."""
        if self._is_talentbrew is not None:
            return self._is_talentbrew

        for indicator in TALENTBREW_INDICATORS:
            if indicator in html:
                self._is_talentbrew = True
                return True

        self._is_talentbrew = False
        return False

    # ──────────────────────────────────────────────
    # Job Listing
    # ──────────────────────────────────────────────

    def _fetch_search_page(self, page: int = 1, keyword: str = "") -> str:
        """Fetch a search results page."""
        # TalentBrew search URL pattern
        url = f"{self._base_url}/search-jobs"
        params = {"p": page}
        if keyword:
            params["k"] = keyword

        resp = self._get(url, params=params)
        return resp.text

    def _parse_job_links(self, html: str) -> list[dict]:
        """Extract job links from search results HTML."""
        soup = BeautifulSoup(html, "lxml")
        jobs = []

        # TalentBrew job listings are in <a> tags with data-job-id
        for link in soup.find_all("a", {"data-job-id": True}):
            job_id = link.get("data-job-id", "")
            href = link.get("href", "")

            if not job_id or not href:
                continue

            # Extract basic info from the listing
            title_elem = link.find("h2")
            title = title_elem.get_text(strip=True) if title_elem else ""

            location_elem = link.find("span", class_="job-location")
            location = location_elem.get_text(strip=True) if location_elem else ""

            org_elem = link.find("span", class_="job-organization")
            organization = org_elem.get_text(strip=True) if org_elem else ""

            category_elem = link.find("span", class_="job-category")
            category = category_elem.get_text(strip=True) if category_elem else ""

            jobs.append({
                "job_id": job_id,
                "url": urljoin(self._base_url, href),
                "title": title,
                "location": location,
                "organization": organization,
                "category": category,
            })

        return jobs

    def _get_total_jobs(self, html: str) -> Optional[int]:
        """Extract total job count from search page."""
        # Look for meta tag with total count
        match = re.search(r'search-analytics-total-jobs"\s*content="(\d+)"', html)
        if match:
            return int(match.group(1))

        # Fallback: look for "X jobs found" text
        match = re.search(r'(\d+)\s+jobs?\s+found', html, re.IGNORECASE)
        if match:
            return int(match.group(1))

        return None

    # ──────────────────────────────────────────────
    # Job Detail
    # ──────────────────────────────────────────────

    def _fetch_job_detail(self, url: str) -> str:
        """Fetch a job detail page."""
        resp = self._get(url)
        return resp.text

    def _extract_json_ld(self, html: str) -> Optional[dict]:
        """Extract JSON-LD JobPosting from HTML."""
        soup = BeautifulSoup(html, "lxml")

        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and data.get("@type") == "JobPosting":
                    return data
                # Handle array of structured data
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "JobPosting":
                            return item
            except (json.JSONDecodeError, TypeError):
                continue

        return None

    def _parse_job_detail(self, json_ld: dict, listing: dict) -> Job:
        """Parse JSON-LD JobPosting into a Job object."""
        # Title
        title = json_ld.get("title", "") or listing.get("title", "")

        # Job ID - extract from identifier or URL
        job_id = str(json_ld.get("identifier", "")) or listing.get("job_id", "")

        # Location
        location = self._extract_location(json_ld)
        if not location:
            location = listing.get("location", "")

        # Posted date
        posted_date = None
        date_str = json_ld.get("datePosted", "")
        if date_str:
            try:
                posted_date = parse_date(date_str)
            except (ValueError, TypeError):
                pass

        # Employment type
        job_type = json_ld.get("employmentType", "")
        if isinstance(job_type, list):
            job_type = ", ".join(job_type)

        # Description - strip HTML
        description = self._strip_html(json_ld.get("description", ""))

        # Qualifications
        qualifications = self._strip_html(json_ld.get("qualifications", ""))

        # Salary
        salary_range = self._extract_salary(json_ld)

        # URL
        url = json_ld.get("url", "") or listing.get("url", "")

        # Organization
        org = json_ld.get("hiringOrganization", {})
        company_name = ""
        if isinstance(org, dict):
            company_name = org.get("name", "")
        if not company_name:
            company_name = listing.get("organization", "") or self.company.name

        # Work hours
        work_hours = json_ld.get("workHours", "")

        return Job(
            id=job_id,
            source_ats="talentbrew",
            company_name=company_name,
            title=title,
            department=listing.get("category", ""),
            location=location,
            job_type=job_type or work_hours,
            posted_date=posted_date,
            url=url,
            description=description,
            qualifications=qualifications,
            salary_range=salary_range,
            categories=[],
            raw_data=json_ld,
        )

    def _extract_location(self, json_ld: dict) -> str:
        """Extract location string from JSON-LD jobLocation."""
        locations = json_ld.get("jobLocation", [])
        if not locations:
            return ""

        if not isinstance(locations, list):
            locations = [locations]

        parts = []
        for loc in locations:
            if not isinstance(loc, dict):
                continue

            address = loc.get("address", {})
            if not isinstance(address, dict):
                continue

            city = address.get("addressLocality", "")
            state = address.get("addressRegion", "")
            country = address.get("addressCountry", "")

            if city and state:
                parts.append(f"{city}, {state}")
            elif city:
                parts.append(city)
            elif state:
                parts.append(state)

        return "; ".join(parts)

    def _extract_salary(self, json_ld: dict) -> Optional[str]:
        """Extract salary information from JSON-LD."""
        base_salary = json_ld.get("baseSalary", {})
        if not isinstance(base_salary, dict):
            return None

        currency = base_salary.get("currency", "USD")
        value = base_salary.get("value", {})

        if not isinstance(value, dict):
            return None

        min_val = value.get("minValue")
        max_val = value.get("maxValue")
        unit = value.get("unitText", "")

        if min_val and max_val:
            return f"${min_val:,.0f} - ${max_val:,.0f} {unit}".strip()
        elif min_val:
            return f"${min_val:,.0f} {unit}".strip()
        elif max_val:
            return f"${max_val:,.0f} {unit}".strip()

        return None

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

    # ──────────────────────────────────────────────
    # Public Interface (implements BaseScraper)
    # ──────────────────────────────────────────────

    def discover_jobs(self, keyword: Optional[str] = None, **kwargs) -> list[Job]:
        """Discover all job listings from this portal."""
        all_listings = []
        page = 1
        total_jobs = None

        while page <= MAX_PAGES:
            logger.debug(f"Fetching TalentBrew page {page}...")

            try:
                html = self._fetch_search_page(page=page, keyword=keyword or "")
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

            # Verify this is a TalentBrew site on first page
            if page == 1:
                if not self._is_talentbrew_site(html):
                    logger.warning(
                        f"{self.company.name} does not appear to be a TalentBrew site"
                    )
                    # Continue anyway - might still work

                total_jobs = self._get_total_jobs(html)
                if total_jobs:
                    logger.info(f"TalentBrew reports {total_jobs} total jobs")

            # Parse job listings
            listings = self._parse_job_links(html)
            if not listings:
                logger.debug("No more jobs found, stopping pagination")
                break

            all_listings.extend(listings)
            logger.debug(f"Page {page}: found {len(listings)} jobs")

            # Check if we've got all jobs
            if total_jobs and len(all_listings) >= total_jobs:
                break

            if len(all_listings) >= MAX_JOBS:
                logger.warning(f"Hit safety limit of {MAX_JOBS} jobs")
                break

            page += 1

        logger.info(f"Discovered {len(all_listings)} job listings from TalentBrew")

        # Convert to partial Job objects
        jobs = []
        for listing in all_listings:
            job = Job(
                id=listing["job_id"],
                source_ats="talentbrew",
                company_name=listing.get("organization", "") or self.company.name,
                title=listing["title"],
                location=listing["location"],
                url=listing["url"],
                raw_data={"listing": listing},
            )
            jobs.append(job)

        return jobs

    def scrape_job_detail(self, job: Job) -> Job:
        """Fetch full details for a single job posting."""
        if not job.url:
            logger.warning(f"Job {job.id} missing URL, skipping detail fetch")
            return job

        listing = {}
        if job.raw_data and "listing" in job.raw_data:
            listing = job.raw_data["listing"]

        try:
            html = self._fetch_job_detail(job.url)
            json_ld = self._extract_json_ld(html)

            if json_ld:
                enriched = self._parse_job_detail(json_ld, listing)
                return enriched
            else:
                logger.warning(f"No JSON-LD found for job {job.id}")
                return job

        except Exception as e:
            logger.error(f"Failed to fetch job detail for {job.id}: {e}")
            return job
