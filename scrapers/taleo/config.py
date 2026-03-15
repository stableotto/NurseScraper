"""Configuration for Taleo scraper."""

# Default page size for job listings
DEFAULT_PAGE_SIZE = 25

# Maximum pages to fetch
MAX_PAGES = 40

# Maximum jobs to discover
MAX_JOBS = 1000

# Taleo URL patterns
# careersection/{section}/joblist.ftl - job list
# careersection/{section}/jobdetail.ftl?job={id} - job detail

# Field indices in the fillList array for job listings (listRequisition)
LIST_FIELDS = {
    "job_id": 3,
    "title": 4,
    "requisition_number": 11,
    "location": 12,
    "category": 19,
    "job_type": 20,
    "posted_date": 21,
    "department": 22,
}

# Field indices in the fillList array for job details (descRequisition)
DETAIL_FIELDS = {
    "job_id": 0,
    "title": 9,
    "requisition_number": 10,
    "description": 11,  # URL-encoded HTML with !*! prefix
    "qualifications": 12,  # URL-encoded HTML with !*! prefix
    "responsibilities": 13,  # URL-encoded HTML with !*! prefix
    "location": 15,
    "job_category": 19,
    "department": 21,
    "job_type": 23,
    "employment_type": 25,
    "posted_date": 31,
    "work_schedule": 33,
    "shift": 37,
    "specialty": 43,
}
