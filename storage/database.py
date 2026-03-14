"""
SQLite canonical data store for portals and jobs.

Provides connection management, schema creation, and upsert helpers.
All discovery and scraping pipelines write here as the single source of truth.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS portals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    subdomain     TEXT UNIQUE NOT NULL,
    slug          TEXT NOT NULL,
    name          TEXT,
    url           TEXT,
    ats_type      TEXT DEFAULT 'icims',
    sector        TEXT,
    state         TEXT,
    city          TEXT,
    verified      BOOLEAN DEFAULT 0,
    discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_seen     DATETIME
);

CREATE TABLE IF NOT EXISTS jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    portal_id      INTEGER REFERENCES portals(id),
    external_id    TEXT,
    title          TEXT NOT NULL,
    department     TEXT,
    location       TEXT,
    state          TEXT,
    city           TEXT,
    job_type       TEXT,
    salary_min     REAL,
    salary_max     REAL,
    posted_date    DATE,
    url            TEXT,
    description    TEXT,
    qualifications TEXT,
    is_nursing     BOOLEAN,
    categories     TEXT,
    scraped_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    unique_key     TEXT UNIQUE,
    UNIQUE(portal_id, external_id)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type    TEXT NOT NULL,
    started_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME,
    portals_found   INTEGER DEFAULT 0,
    jobs_found      INTEGER DEFAULT 0,
    feeds_generated INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'running',
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_portal   ON jobs(portal_id);
CREATE INDEX IF NOT EXISTS idx_jobs_state    ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_nursing  ON jobs(is_nursing);
CREATE INDEX IF NOT EXISTS idx_jobs_posted   ON jobs(posted_date);
CREATE INDEX IF NOT EXISTS idx_jobs_title    ON jobs(title);
CREATE INDEX IF NOT EXISTS idx_portals_sector ON portals(sector);
CREATE INDEX IF NOT EXISTS idx_portals_state  ON portals(state);
CREATE INDEX IF NOT EXISTS idx_portals_ats    ON portals(ats_type);
"""


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection with WAL mode and foreign keys enabled."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db_session(db_path: str | Path):
    """Context manager yielding a connection that auto-commits or rolls back."""
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | Path) -> None:
    """Create tables and indexes if they don't exist."""
    with db_session(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
    logger.info(f"Database initialized at {db_path}")


# ──────────────────────────────────────────────
# Portal upserts
# ──────────────────────────────────────────────

def upsert_portal(
    conn: sqlite3.Connection,
    *,
    subdomain: str,
    slug: str,
    name: str = "",
    url: str = "",
    ats_type: str = "icims",
    sector: str = "",
    state: str = "",
    city: str = "",
    verified: bool = False,
) -> int:
    """Insert or update a portal. Returns the portal row id."""
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO portals (subdomain, slug, name, url, ats_type, sector, state, city, verified, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(subdomain) DO UPDATE SET
            name      = COALESCE(NULLIF(excluded.name, ''), portals.name),
            url       = COALESCE(NULLIF(excluded.url, ''), portals.url),
            sector    = COALESCE(NULLIF(excluded.sector, ''), portals.sector),
            state     = COALESCE(NULLIF(excluded.state, ''), portals.state),
            city      = COALESCE(NULLIF(excluded.city, ''), portals.city),
            verified  = MAX(portals.verified, excluded.verified),
            last_seen = excluded.last_seen
        """,
        (subdomain, slug, name, url, ats_type, sector, state, city, int(verified), now),
    )
    row = conn.execute(
        "SELECT id FROM portals WHERE subdomain = ?", (subdomain,)
    ).fetchone()
    return row["id"]


def bulk_upsert_portals(
    conn: sqlite3.Connection,
    portals: list[dict],
) -> int:
    """Upsert many portals at once. Returns count of rows affected."""
    count = 0
    for p in portals:
        upsert_portal(conn, **p)
        count += 1
    return count


def get_portal_id(conn: sqlite3.Connection, subdomain: str) -> Optional[int]:
    """Look up a portal id by subdomain."""
    row = conn.execute(
        "SELECT id FROM portals WHERE subdomain = ?", (subdomain,)
    ).fetchone()
    return row["id"] if row else None


# ──────────────────────────────────────────────
# Job upserts
# ──────────────────────────────────────────────

def _parse_salary(salary_range: Optional[str]) -> tuple[Optional[float], Optional[float]]:
    """Best-effort extraction of min/max from a salary string like '$50,000 - $70,000'."""
    if not salary_range:
        return None, None
    import re
    nums = re.findall(r"[\d,]+\.?\d*", salary_range.replace(",", ""))
    floats = []
    for n in nums:
        try:
            floats.append(float(n))
        except ValueError:
            continue
    if len(floats) >= 2:
        return min(floats), max(floats)
    if len(floats) == 1:
        return floats[0], None
    return None, None


def upsert_job(
    conn: sqlite3.Connection,
    *,
    portal_id: int,
    external_id: str,
    title: str,
    unique_key: str,
    department: str = "",
    location: str = "",
    state: str = "",
    city: str = "",
    job_type: str = "",
    salary_min: Optional[float] = None,
    salary_max: Optional[float] = None,
    posted_date: Optional[str] = None,
    url: str = "",
    description: str = "",
    qualifications: str = "",
    is_nursing: bool = False,
    categories: Optional[list[str]] = None,
) -> int:
    """Insert or update a job. Returns the job row id."""
    cats_json = json.dumps(categories or [])
    now = datetime.utcnow().isoformat()
    conn.execute(
        """
        INSERT INTO jobs (
            portal_id, external_id, title, unique_key,
            department, location, state, city, job_type,
            salary_min, salary_max, posted_date, url,
            description, qualifications, is_nursing, categories, scraped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(unique_key) DO UPDATE SET
            title          = excluded.title,
            department     = excluded.department,
            location       = excluded.location,
            state          = COALESCE(NULLIF(excluded.state, ''), jobs.state),
            city           = COALESCE(NULLIF(excluded.city, ''), jobs.city),
            job_type       = COALESCE(NULLIF(excluded.job_type, ''), jobs.job_type),
            salary_min     = COALESCE(excluded.salary_min, jobs.salary_min),
            salary_max     = COALESCE(excluded.salary_max, jobs.salary_max),
            posted_date    = COALESCE(excluded.posted_date, jobs.posted_date),
            url            = COALESCE(NULLIF(excluded.url, ''), jobs.url),
            description    = COALESCE(NULLIF(excluded.description, ''), jobs.description),
            qualifications = COALESCE(NULLIF(excluded.qualifications, ''), jobs.qualifications),
            is_nursing     = excluded.is_nursing,
            categories     = excluded.categories,
            scraped_at     = excluded.scraped_at
        """,
        (
            portal_id, external_id, title, unique_key,
            department, location, state, city, job_type,
            salary_min, salary_max, posted_date, url,
            description, qualifications, int(is_nursing), cats_json, now,
        ),
    )
    row = conn.execute(
        "SELECT id FROM jobs WHERE unique_key = ?", (unique_key,)
    ).fetchone()
    return row["id"]


def bulk_upsert_jobs(conn: sqlite3.Connection, jobs: list[dict]) -> int:
    """Upsert many jobs at once. Returns count of rows affected."""
    count = 0
    for j in jobs:
        upsert_job(conn, **j)
        count += 1
    return count


# ──────────────────────────────────────────────
# Scrape run tracking
# ──────────────────────────────────────────────

def start_run(conn: sqlite3.Connection, run_type: str) -> int:
    """Record the start of a pipeline run. Returns the run id."""
    cur = conn.execute(
        "INSERT INTO scrape_runs (run_type) VALUES (?)", (run_type,)
    )
    conn.commit()
    return cur.lastrowid


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    portals_found: int = 0,
    jobs_found: int = 0,
    feeds_generated: int = 0,
    status: str = "completed",
    error: Optional[str] = None,
) -> None:
    """Record the end of a pipeline run."""
    conn.execute(
        """
        UPDATE scrape_runs SET
            finished_at     = CURRENT_TIMESTAMP,
            portals_found   = ?,
            jobs_found      = ?,
            feeds_generated = ?,
            status          = ?,
            error           = ?
        WHERE id = ?
        """,
        (portals_found, jobs_found, feeds_generated, status, error, run_id),
    )
    conn.commit()


# ──────────────────────────────────────────────
# Query helpers (used by feed generator & future API)
# ──────────────────────────────────────────────

def query_jobs(
    conn: sqlite3.Connection,
    *,
    sectors: Optional[list[str]] = None,
    states: Optional[list[str]] = None,
    is_nursing: Optional[bool] = None,
    categories: Optional[list[str]] = None,
    title_keywords: Optional[list[str]] = None,
    exclude_keywords: Optional[list[str]] = None,
    posted_within_days: Optional[int] = None,
    salary_min: Optional[float] = None,
    ats_types: Optional[list[str]] = None,
    limit: Optional[int] = None,
) -> list[sqlite3.Row]:
    """Flexible job query driven by filter parameters."""
    clauses = []
    params: list = []

    if sectors:
        placeholders = ",".join("?" * len(sectors))
        clauses.append(f"p.sector IN ({placeholders})")
        params.extend(sectors)

    if states:
        placeholders = ",".join("?" * len(states))
        clauses.append(f"j.state IN ({placeholders})")
        params.extend(states)

    if is_nursing is not None:
        clauses.append("j.is_nursing = ?")
        params.append(int(is_nursing))

    if categories:
        # categories is stored as JSON array, use LIKE for each category
        cat_parts = ["j.categories LIKE ?" for _ in categories]
        clauses.append(f"({' OR '.join(cat_parts)})")
        params.extend(f'%"{cat}"%' for cat in categories)

    if title_keywords:
        kw_parts = ["j.title LIKE ?" for _ in title_keywords]
        clauses.append(f"({' OR '.join(kw_parts)})")
        params.extend(f"%{kw}%" for kw in title_keywords)

    if exclude_keywords:
        for kw in exclude_keywords:
            clauses.append("j.title NOT LIKE ?")
            params.append(f"%{kw}%")

    if posted_within_days is not None:
        clauses.append("j.posted_date >= date('now', ?)")
        params.append(f"-{posted_within_days} days")

    if salary_min is not None:
        clauses.append("(j.salary_min >= ? OR j.salary_max >= ?)")
        params.extend([salary_min, salary_min])

    if ats_types:
        placeholders = ",".join("?" * len(ats_types))
        clauses.append(f"p.ats_type IN ({placeholders})")
        params.extend(ats_types)

    where = " AND ".join(clauses) if clauses else "1=1"

    sql = f"""
        SELECT
            j.id, j.external_id, j.title, j.department, j.location,
            j.state, j.city, j.job_type,
            j.salary_min, j.salary_max, j.posted_date,
            j.url, j.description, j.qualifications,
            j.is_nursing, j.categories, j.scraped_at, j.unique_key,
            p.name  AS company_name,
            p.subdomain, p.slug AS portal_slug,
            p.sector, p.ats_type
        FROM jobs j
        JOIN portals p ON j.portal_id = p.id
        WHERE {where}
        ORDER BY j.posted_date DESC, j.id DESC
    """

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, params).fetchall()
