"""Workday-specific configuration."""

# Workday uses a JSON API at /wday/cxs/{tenant}/{site}/jobs
# Job details are fetched via GET /wday/cxs/{tenant}/{site}/job/{path}

# API paths
WORKDAY_JOBS_PATH = "/wday/cxs/{tenant}/{site}/jobs"
WORKDAY_JOB_DETAIL_PATH = "/wday/cxs/{tenant}/{site}/job"

# Domain pattern: {tenant}.{wd_instance}.myworkdayjobs.com
WORKDAY_DOMAIN_SUFFIX = ".myworkdayjobs.com"

# Default page size (Workday CXS API supports up to 20)
DEFAULT_PAGE_SIZE = 20

# Maximum jobs to fetch (safety limit)
MAX_JOBS = 2000

# Common facet parameters for filtering
FACET_JOB_FAMILY = "jobFamilyGroup"
FACET_WORKER_TYPE = "workerSubType"
FACET_TIME_TYPE = "timeType"
FACET_LOCATION = "locations"
FACET_REMOTE = "remoteType"

# Nursing-related job family descriptors in Workday
NURSING_JOB_FAMILIES = [
    "Nursing",
    "Clinical",
    "Patient Care",
    "Allied Health",
    "Healthcare",
]
