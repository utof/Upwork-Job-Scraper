"""
Simple SQLite database layer for job storage.

Thin abstraction over raw SQLite - easy to swap to PostgreSQL or add ORM later.
All DB logic lives here, rest of codebase just calls these functions.
"""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any

# Default database path (can be overridden)
DEFAULT_DB_PATH = Path(__file__).parent.parent / 'data' / 'jobs.db'


@contextmanager
def get_connection(db_path: Path = DEFAULT_DB_PATH):
    """Context manager for database connections."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Access columns by name
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db(db_path: Path = DEFAULT_DB_PATH):
    """Create tables if they don't exist."""
    with get_connection(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                run_id TEXT,
                search_query TEXT,
                title TEXT,
                description TEXT,
                url TEXT,
                location_restriction TEXT,
                client_country TEXT,
                type TEXT,
                hourly_min TEXT,
                hourly_max TEXT,
                fixed_budget_amount TEXT,
                duration TEXT,
                level TEXT,
                category TEXT,
                skills TEXT,
                client_total_spent TEXT,
                client_hires TEXT,
                client_rating TEXT,
                payment_verified INTEGER,
                score REAL,
                ai_analysis TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_data TEXT
            )
        """)
        # Index for quick existence checks
        conn.execute('CREATE INDEX IF NOT EXISTS idx_job_id ON jobs(job_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_run_id ON jobs(run_id)')


def job_exists(job_id: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Check if a job already exists in the database."""
    with get_connection(db_path) as conn:
        result = conn.execute(
            'SELECT 1 FROM jobs WHERE job_id = ? LIMIT 1', (job_id,)
        ).fetchone()
        return result is not None


def insert_job(
    job_data: dict[str, Any],
    run_id: str = None,
    search_query: str = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> bool:
    """
    Insert a job into the database.
    Returns True if inserted, False if already exists.
    """
    if job_exists(job_data.get('job_id', ''), db_path):
        return False

    with get_connection(db_path) as conn:
        # Store full raw data as JSON for future-proofing
        raw_data = json.dumps(job_data, default=str)

        # Convert skills list to JSON string if it's a list
        skills = job_data.get('skills', '')
        if isinstance(skills, list):
            skills = json.dumps(skills)

        conn.execute(
            """
            INSERT OR IGNORE INTO jobs (
                job_id, run_id, search_query, title, description, url, location_restriction,
                client_country, type, hourly_min, hourly_max, fixed_budget_amount,
                duration, level, category, skills, client_total_spent,
                client_hires, client_rating, payment_verified, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                job_data.get('job_id'),
                run_id,
                search_query,
                job_data.get('title'),
                job_data.get('description'),
                job_data.get('url'),
                job_data.get('location_restriction'),
                job_data.get('client_country'),
                job_data.get('type'),
                job_data.get('hourly_min'),
                job_data.get('hourly_max'),
                job_data.get('fixed_budget_amount'),
                job_data.get('duration'),
                job_data.get('level'),
                job_data.get('category'),
                skills,
                job_data.get('client_total_spent'),
                job_data.get('client_hires'),
                job_data.get('client_rating'),
                1 if job_data.get('payment_verified') else 0,
                raw_data,
            ),
        )
        return True


def insert_jobs_batch(
    jobs: list[dict[str, Any]],
    run_id: str = None,
    search_query: str = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> int:
    """
    Insert multiple jobs, stopping when we hit an existing one.
    Returns number of new jobs inserted.

    Since jobs are sorted by newest first, once we hit a known job,
    all subsequent jobs are also known.
    """
    inserted_count = 0
    for job in jobs:
        job_id = job.get('job_id', '')
        if not job_id:
            continue
        if job_exists(job_id, db_path):
            # Hit a known job - stop processing
            break
        if insert_job(job, run_id, search_query, db_path):
            inserted_count += 1
    return inserted_count


def get_job(job_id: str, db_path: Path = DEFAULT_DB_PATH) -> dict | None:
    """Get a single job by ID."""
    with get_connection(db_path) as conn:
        row = conn.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
        if row:
            return dict(row)
        return None


def get_recent_jobs(limit: int = 50, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get most recent jobs."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            'SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?', (limit,)
        ).fetchall()
        return [dict(row) for row in rows]


def get_unanalyzed_jobs(limit: int = 10, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get jobs that haven't been analyzed by AI yet."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            'SELECT * FROM jobs WHERE ai_analysis IS NULL ORDER BY created_at DESC LIMIT ?',
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def update_job_score(job_id: str, score: float, db_path: Path = DEFAULT_DB_PATH):
    """Update the score for a job."""
    with get_connection(db_path) as conn:
        conn.execute('UPDATE jobs SET score = ? WHERE job_id = ?', (score, job_id))


def update_job_analysis(job_id: str, analysis: str, db_path: Path = DEFAULT_DB_PATH):
    """Update the AI analysis for a job."""
    with get_connection(db_path) as conn:
        conn.execute(
            'UPDATE jobs SET ai_analysis = ? WHERE job_id = ?', (analysis, job_id)
        )


def get_job_count(db_path: Path = DEFAULT_DB_PATH) -> int:
    """Get total number of jobs in database."""
    with get_connection(db_path) as conn:
        result = conn.execute('SELECT COUNT(*) FROM jobs').fetchone()
        return result[0] if result else 0


def delete_by_run_id(run_id: str, db_path: Path = DEFAULT_DB_PATH) -> int:
    """Delete all jobs from a specific run. Returns number deleted."""
    with get_connection(db_path) as conn:
        cursor = conn.execute('DELETE FROM jobs WHERE run_id = ?', (run_id,))
        return cursor.rowcount


def get_jobs_by_run_id(run_id: str, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get all jobs from a specific run."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            'SELECT * FROM jobs WHERE run_id = ? ORDER BY created_at DESC', (run_id,)
        ).fetchall()
        return [dict(row) for row in rows]


def get_high_scoring_jobs(
    threshold: float = 8.0, limit: int = 10, db_path: Path = DEFAULT_DB_PATH
) -> list[dict]:
    """Get jobs with score >= threshold for proposal generation."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            'SELECT * FROM jobs WHERE score >= ? ORDER BY score DESC LIMIT ?',
            (threshold, limit),
        ).fetchall()
        return [dict(row) for row in rows]
