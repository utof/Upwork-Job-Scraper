"""
FastAPI server for Job Review UI.

Provides REST API endpoints for the job review interface.
Run with: uv run server.py
"""

import json
from pathlib import Path
from typing import Annotated

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from utils.ai_scorer import score_job
from utils.db import (
    DEFAULT_DB_PATH,
    dismiss_job,
    get_active_job_count,
    get_active_jobs,
    get_job,
    get_scoring_stats,
    init_db,
    migrate_add_dismiss_columns,
    restore_job,
    update_job_analysis,
    update_job_score,
)

app = FastAPI(title='Upwork Job Review API')

# Static files directory
STATIC_DIR = Path(__file__).parent / 'static'


# Pydantic models for request/response
class DismissRequest(BaseModel):
    reason: str | None = None


class ScoreResponse(BaseModel):
    score: float
    analysis: dict


class JobListResponse(BaseModel):
    jobs: list[dict]
    total: int
    limit: int
    offset: int


# API endpoints
@app.get('/api/jobs')
def list_jobs(
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
    sort: Annotated[str, Query()] = 'newest',
    min_score: Annotated[float | None, Query()] = None,
) -> JobListResponse:
    """List active (non-dismissed) jobs with pagination and sorting."""
    jobs = get_active_jobs(limit=limit, offset=offset, sort=sort, min_score=min_score)
    total = get_active_job_count(min_score=min_score)

    # Parse ai_analysis JSON string to dict for each job
    for job in jobs:
        if job.get('ai_analysis'):
            try:
                job['ai_analysis'] = json.loads(job['ai_analysis'])
            except json.JSONDecodeError:
                pass
        # Parse skills JSON if it's a string
        if job.get('skills') and isinstance(job['skills'], str):
            try:
                job['skills'] = json.loads(job['skills'])
            except json.JSONDecodeError:
                pass

    return JobListResponse(jobs=jobs, total=total, limit=limit, offset=offset)


@app.get('/api/jobs/{job_id}')
def get_job_detail(job_id: str) -> dict:
    """Get a single job by ID."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')

    # Parse ai_analysis JSON string to dict
    if job.get('ai_analysis'):
        try:
            job['ai_analysis'] = json.loads(job['ai_analysis'])
        except json.JSONDecodeError:
            pass
    # Parse skills JSON
    if job.get('skills') and isinstance(job['skills'], str):
        try:
            job['skills'] = json.loads(job['skills'])
        except json.JSONDecodeError:
            pass

    return job


@app.post('/api/jobs/{job_id}/dismiss')
def dismiss_job_endpoint(job_id: str, request: DismissRequest = None) -> dict:
    """Soft-delete a job with optional reason."""
    reason = request.reason if request else None
    success = dismiss_job(job_id, reason)
    if not success:
        raise HTTPException(status_code=404, detail='Job not found')
    return {'status': 'dismissed', 'job_id': job_id}


@app.post('/api/jobs/{job_id}/restore')
def restore_job_endpoint(job_id: str) -> dict:
    """Restore a dismissed job."""
    success = restore_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail='Job not found')
    return {'status': 'restored', 'job_id': job_id}


@app.post('/api/jobs/{job_id}/score')
def score_job_endpoint(job_id: str) -> ScoreResponse:
    """Trigger AI scoring for a single job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')

    try:
        score, analysis = score_job(job)
        update_job_score(job_id, score)
        update_job_analysis(job_id, json.dumps(analysis))
        return ScoreResponse(score=score, analysis=analysis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Scoring failed: {e}')


@app.get('/api/stats')
def get_stats() -> dict:
    """Get scoring statistics."""
    return get_scoring_stats()


# Serve static files and index.html
@app.get('/')
def serve_index():
    """Serve the main index.html file."""
    index_path = STATIC_DIR / 'index.html'
    if not index_path.exists():
        raise HTTPException(status_code=404, detail='index.html not found')
    return FileResponse(index_path)


# Mount static files after specific routes
if STATIC_DIR.exists():
    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')


@app.on_event('startup')
def startup_event():
    """Initialize database on startup."""
    init_db()
    migrate_add_dismiss_columns()
    print(f'Database initialized at {DEFAULT_DB_PATH}')


if __name__ == '__main__':
    uvicorn.run('server:app', host='0.0.0.0', port=8000, reload=True)
