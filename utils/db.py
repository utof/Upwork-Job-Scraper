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
                type TEXT,

                -- Budget/rate fields
                hourly_min TEXT,
                hourly_max TEXT,
                fixed_budget_amount TEXT,
                currency TEXT,

                -- Job details
                duration TEXT,
                level TEXT,
                category TEXT,
                category_name TEXT,
                category_urlSlug TEXT,
                categoryGroup_name TEXT,
                categoryGroup_urlSlug TEXT,
                skills TEXT,
                qualifications TEXT,
                questions TEXT,
                location_restriction TEXT,
                connects_required INTEGER,
                contractorTier TEXT,
                numberOfPositionsToHire INTEGER,
                applicants INTEGER,

                -- Job flags
                premium INTEGER,
                enterpriseJob INTEGER,
                isContractToHire INTEGER,

                -- Client/buyer info
                client_country TEXT,
                client_total_spent TEXT,
                client_hires TEXT,
                client_rating TEXT,
                client_reviews TEXT,
                client_company_size TEXT,
                client_industry TEXT,
                payment_verified INTEGER,
                phone_verified INTEGER,

                -- Buyer location
                buyer_location_city TEXT,
                buyer_location_countryTimezone TEXT,
                buyer_location_localTime TEXT,
                buyer_location_offsetFromUtcMillis INTEGER,

                -- Buyer stats
                buyer_avgHourlyJobsRate_amount TEXT,
                buyer_company_contractDate TEXT,
                buyer_hire_rate_pct INTEGER,
                buyer_jobs_openCount INTEGER,
                buyer_jobs_postedCount INTEGER,
                buyer_stats_activeAssignmentsCount INTEGER,
                buyer_stats_hoursCount TEXT,
                buyer_stats_totalJobsWithHires INTEGER,

                -- Client activity
                clientActivity_invitationsSent INTEGER,
                clientActivity_totalHired INTEGER,
                clientActivity_totalInvitedToInterview INTEGER,
                clientActivity_unansweredInvites INTEGER,
                lastBuyerActivity TEXT,

                -- Timestamps
                ts_create INTEGER,
                posted_at INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- AI scoring
                score REAL,
                ai_analysis TEXT,

                -- Soft delete
                dismissed_at TIMESTAMP,
                dismiss_reason TEXT,

                -- Full raw data for future-proofing
                raw_data TEXT
            )
        """)
        # Index for quick existence checks
        conn.execute('CREATE INDEX IF NOT EXISTS idx_job_id ON jobs(job_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_run_id ON jobs(run_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_posted_at ON jobs(posted_at)')


def job_exists(job_id: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Check if a job already exists in the database."""
    with get_connection(db_path) as conn:
        result = conn.execute(
            'SELECT 1 FROM jobs WHERE job_id = ? LIMIT 1', (job_id,)
        ).fetchone()
        return result is not None


def _to_int(value) -> int | None:
    """Convert value to int, return None if not possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _to_timestamp(value) -> int | None:
    """Convert various timestamp formats to unix timestamp int."""
    if value is None:
        return None
    # Already an int/float
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        # Try as unix timestamp string first
        try:
            return int(float(value))
        except (ValueError, TypeError):
            pass
        # Try ISO format (e.g., "2026-01-25T10:49:07.750Z")
        try:
            from datetime import datetime

            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return None


def _to_bool_int(value) -> int:
    """Convert truthy value to 1/0 for SQLite."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, str):
        return 1 if value.lower() in ('true', '1', 'yes') else 0
    return 1 if value else 0


def _to_json(value) -> str | None:
    """Convert list/dict to JSON string."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return str(value) if value else None


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

        conn.execute(
            """
            INSERT OR IGNORE INTO jobs (
                job_id, run_id, search_query, title, description, url, type,
                hourly_min, hourly_max, fixed_budget_amount, currency,
                duration, level, category, category_name, category_urlSlug,
                categoryGroup_name, categoryGroup_urlSlug, skills, qualifications,
                questions, location_restriction, connects_required, contractorTier,
                numberOfPositionsToHire, applicants,
                premium, enterpriseJob, isContractToHire,
                client_country, client_total_spent, client_hires, client_rating,
                client_reviews, client_company_size, client_industry,
                payment_verified, phone_verified,
                buyer_location_city, buyer_location_countryTimezone,
                buyer_location_localTime, buyer_location_offsetFromUtcMillis,
                buyer_avgHourlyJobsRate_amount, buyer_company_contractDate,
                buyer_hire_rate_pct, buyer_jobs_openCount, buyer_jobs_postedCount,
                buyer_stats_activeAssignmentsCount, buyer_stats_hoursCount,
                buyer_stats_totalJobsWithHires,
                clientActivity_invitationsSent, clientActivity_totalHired,
                clientActivity_totalInvitedToInterview, clientActivity_unansweredInvites,
                lastBuyerActivity, ts_create, posted_at, raw_data
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?
            )
        """,
            (
                # Basic info
                job_data.get('job_id'),
                run_id,
                search_query,
                job_data.get('title'),
                job_data.get('description'),
                job_data.get('url'),
                job_data.get('type'),
                # Budget/rate
                job_data.get('hourly_min'),
                job_data.get('hourly_max'),
                job_data.get('fixed_budget_amount'),
                job_data.get('currency'),
                # Job details
                job_data.get('duration'),
                job_data.get('level'),
                job_data.get('category'),
                job_data.get('category_name'),
                job_data.get('category_urlSlug'),
                job_data.get('categoryGroup_name'),
                job_data.get('categoryGroup_urlSlug'),
                _to_json(job_data.get('skills')),
                _to_json(job_data.get('qualifications')),
                _to_json(job_data.get('questions')),
                job_data.get('location_restriction'),
                _to_int(job_data.get('connects_required')),
                job_data.get('contractorTier'),
                _to_int(job_data.get('numberOfPositionsToHire')),
                _to_int(job_data.get('applicants')),
                # Job flags
                _to_bool_int(job_data.get('premium')),
                _to_bool_int(job_data.get('enterpriseJob')),
                _to_bool_int(job_data.get('isContractToHire')),
                # Client info
                job_data.get('client_country'),
                job_data.get('client_total_spent'),
                job_data.get('client_hires'),
                job_data.get('client_rating'),
                job_data.get('client_reviews'),
                job_data.get('client_company_size'),
                job_data.get('client_industry'),
                _to_bool_int(job_data.get('payment_verified')),
                _to_bool_int(job_data.get('phone_verified')),
                # Buyer location
                job_data.get('buyer_location_city'),
                job_data.get('buyer_location_countryTimezone'),
                job_data.get('buyer_location_localTime'),
                _to_int(job_data.get('buyer_location_offsetFromUtcMillis')),
                # Buyer stats
                job_data.get('buyer_avgHourlyJobsRate_amount'),
                job_data.get('buyer_company_contractDate'),
                _to_int(job_data.get('buyer_hire_rate_pct')),
                _to_int(job_data.get('buyer_jobs_openCount')),
                _to_int(job_data.get('buyer_jobs_postedCount')),
                _to_int(job_data.get('buyer_stats_activeAssignmentsCount')),
                job_data.get('buyer_stats_hoursCount'),
                _to_int(job_data.get('buyer_stats_totalJobsWithHires')),
                # Client activity
                _to_int(job_data.get('clientActivity_invitationsSent')),
                _to_int(job_data.get('clientActivity_totalHired')),
                _to_int(job_data.get('clientActivity_totalInvitedToInterview')),
                _to_int(job_data.get('clientActivity_unansweredInvites')),
                job_data.get('lastBuyerActivity'),
                # Timestamps
                _to_timestamp(job_data.get('ts_create')),
                _to_timestamp(job_data.get('ts_publish')),
                # Raw data
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
    """Get most recently posted jobs."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            'SELECT * FROM jobs ORDER BY COALESCE(posted_at, 0) DESC, created_at DESC LIMIT ?',
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def get_unanalyzed_jobs(limit: int = 10, db_path: Path = DEFAULT_DB_PATH) -> list[dict]:
    """Get jobs that haven't been analyzed by AI yet, newest posted first."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            '''SELECT * FROM jobs WHERE ai_analysis IS NULL
               ORDER BY COALESCE(posted_at, 0) DESC, created_at DESC LIMIT ?''',
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
            'SELECT * FROM jobs WHERE run_id = ? ORDER BY COALESCE(posted_at, 0) DESC, created_at DESC',
            (run_id,),
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


def migrate_db(db_path: Path = DEFAULT_DB_PATH):
    """Run all database migrations to add missing columns."""
    with get_connection(db_path) as conn:
        # Check existing columns
        cursor = conn.execute('PRAGMA table_info(jobs)')
        columns = {row[1] for row in cursor.fetchall()}

        # All columns that should exist (column_name, sql_type)
        required_columns = [
            # Budget/rate
            ('currency', 'TEXT'),
            # Job details
            ('category_name', 'TEXT'),
            ('category_urlSlug', 'TEXT'),
            ('categoryGroup_name', 'TEXT'),
            ('categoryGroup_urlSlug', 'TEXT'),
            ('qualifications', 'TEXT'),
            ('questions', 'TEXT'),
            ('connects_required', 'INTEGER'),
            ('contractorTier', 'TEXT'),
            ('numberOfPositionsToHire', 'INTEGER'),
            ('applicants', 'INTEGER'),
            # Job flags
            ('premium', 'INTEGER'),
            ('enterpriseJob', 'INTEGER'),
            ('isContractToHire', 'INTEGER'),
            # Client info
            ('client_reviews', 'TEXT'),
            ('client_company_size', 'TEXT'),
            ('client_industry', 'TEXT'),
            ('phone_verified', 'INTEGER'),
            # Buyer location
            ('buyer_location_city', 'TEXT'),
            ('buyer_location_countryTimezone', 'TEXT'),
            ('buyer_location_localTime', 'TEXT'),
            ('buyer_location_offsetFromUtcMillis', 'INTEGER'),
            # Buyer stats
            ('buyer_avgHourlyJobsRate_amount', 'TEXT'),
            ('buyer_company_contractDate', 'TEXT'),
            ('buyer_hire_rate_pct', 'INTEGER'),
            ('buyer_jobs_openCount', 'INTEGER'),
            ('buyer_jobs_postedCount', 'INTEGER'),
            ('buyer_stats_activeAssignmentsCount', 'INTEGER'),
            ('buyer_stats_hoursCount', 'TEXT'),
            ('buyer_stats_totalJobsWithHires', 'INTEGER'),
            # Client activity
            ('clientActivity_invitationsSent', 'INTEGER'),
            ('clientActivity_totalHired', 'INTEGER'),
            ('clientActivity_totalInvitedToInterview', 'INTEGER'),
            ('clientActivity_unansweredInvites', 'INTEGER'),
            ('lastBuyerActivity', 'TEXT'),
            # Timestamps
            ('ts_create', 'INTEGER'),
            ('posted_at', 'INTEGER'),
            # Soft delete
            ('dismissed_at', 'TIMESTAMP'),
            ('dismiss_reason', 'TEXT'),
        ]

        # Add missing columns
        for col_name, col_type in required_columns:
            if col_name not in columns:
                conn.execute(f'ALTER TABLE jobs ADD COLUMN {col_name} {col_type}')

        # Create index on posted_at if not exists
        conn.execute('CREATE INDEX IF NOT EXISTS idx_posted_at ON jobs(posted_at)')


# Keep old name as alias for backwards compatibility
migrate_add_dismiss_columns = migrate_db


def dismiss_job(
    job_id: str, reason: str = None, db_path: Path = DEFAULT_DB_PATH
) -> bool:
    """
    Soft-delete a job with optional reason for AI learning.

    Returns True if job was dismissed, False if not found.
    """
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            'UPDATE jobs SET dismissed_at = CURRENT_TIMESTAMP, dismiss_reason = ? WHERE job_id = ?',
            (reason, job_id),
        )
        return cursor.rowcount > 0


def restore_job(job_id: str, db_path: Path = DEFAULT_DB_PATH) -> bool:
    """Restore a dismissed job. Returns True if restored, False if not found."""
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            'UPDATE jobs SET dismissed_at = NULL, dismiss_reason = NULL WHERE job_id = ?',
            (job_id,),
        )
        return cursor.rowcount > 0


def get_active_jobs(
    limit: int = 50,
    offset: int = 0,
    sort: str = 'newest',
    min_score: float = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> list[dict]:
    """
    Get non-dismissed jobs with sorting/filtering.

    Args:
        limit: Max jobs to return
        offset: Pagination offset
        sort: 'newest', 'oldest', 'score_high', 'score_low'
        min_score: Filter by minimum score (optional)

    Returns:
        List of job dicts
    """
    order_clauses = {
        'newest': 'COALESCE(posted_at, 0) DESC, created_at DESC',
        'oldest': 'COALESCE(posted_at, 0) ASC, created_at ASC',
        'score_high': 'score DESC NULLS LAST, COALESCE(posted_at, 0) DESC',
        'score_low': 'score ASC NULLS LAST, COALESCE(posted_at, 0) DESC',
    }
    order_by = order_clauses.get(sort, 'COALESCE(posted_at, 0) DESC, created_at DESC')

    query = 'SELECT * FROM jobs WHERE dismissed_at IS NULL'
    params: list = []

    if min_score is not None:
        query += ' AND score >= ?'
        params.append(min_score)

    query += f' ORDER BY {order_by} LIMIT ? OFFSET ?'
    params.extend([limit, offset])

    with get_connection(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def get_active_job_count(
    min_score: float = None, db_path: Path = DEFAULT_DB_PATH
) -> int:
    """Get count of non-dismissed jobs."""
    query = 'SELECT COUNT(*) FROM jobs WHERE dismissed_at IS NULL'
    params: list = []

    if min_score is not None:
        query += ' AND score >= ?'
        params.append(min_score)

    with get_connection(db_path) as conn:
        result = conn.execute(query, params).fetchone()
        return result[0] if result else 0


def get_scoring_stats(db_path: Path = DEFAULT_DB_PATH) -> dict:
    """Get statistics about job scoring."""
    with get_connection(db_path) as conn:
        stats = {}

        # Total and scored counts
        stats['total_jobs'] = conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]
        stats['scored_jobs'] = conn.execute(
            'SELECT COUNT(*) FROM jobs WHERE score IS NOT NULL'
        ).fetchone()[0]
        stats['dismissed_jobs'] = conn.execute(
            'SELECT COUNT(*) FROM jobs WHERE dismissed_at IS NOT NULL'
        ).fetchone()[0]
        stats['active_jobs'] = conn.execute(
            'SELECT COUNT(*) FROM jobs WHERE dismissed_at IS NULL'
        ).fetchone()[0]

        # Score distribution
        score_result = conn.execute(
            'SELECT AVG(score), MIN(score), MAX(score) FROM jobs WHERE score IS NOT NULL'
        ).fetchone()
        if score_result[0] is not None:
            stats['avg_score'] = round(score_result[0], 2)
            stats['min_score'] = round(score_result[1], 2)
            stats['max_score'] = round(score_result[2], 2)
        else:
            stats['avg_score'] = None
            stats['min_score'] = None
            stats['max_score'] = None

        # High scoring jobs (8+)
        stats['high_scoring'] = conn.execute(
            'SELECT COUNT(*) FROM jobs WHERE score >= 8 AND dismissed_at IS NULL'
        ).fetchone()[0]

        return stats
