"""Configuration for Oracle HCM Recruiting Cloud scraper."""

# Default page size for API requests
DEFAULT_PAGE_SIZE = 25

# Maximum jobs to fetch
MAX_JOBS = 1000

# API endpoint pattern
# Base: https://{tenant}.fa.{region}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions
# Finder: findReqs;siteNumber={site},limit={n},sortBy=POSTING_DATES_DESC

# Common Oracle Cloud regions
ORACLE_REGIONS = [
    "us2",
    "us6",
    "us1",
    "em2",
    "em3",
    "ap1",
]

# Sort options
SORT_OPTIONS = {
    "newest": "POSTING_DATES_DESC",
    "oldest": "POSTING_DATES_ASC",
    "title": "TITLE_ASC",
}
