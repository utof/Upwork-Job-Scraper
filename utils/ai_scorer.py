"""
AI-powered job scoring using OpenRouter API.

Scores jobs 1-10 with focus on meeting-averse jobs for async-first agency.
Uses cheap models for bulk scoring, expensive models reserved for proposal generation.
"""

import json
import os
import time
from pathlib import Path

import httpx
import toml
from dotenv import load_dotenv

# Load .env file from project root
load_dotenv(Path(__file__).parent.parent / '.env')

# Scoring weights - meeting_risk is dominant factor
SCORING_WEIGHTS = {
    'meeting_risk': 0.5,  # 50% - async-first priority
    'scope_clarity': 0.3,  # 30% - clear deliverables
    'agency_fit': 0.2,  # 20% - good for outsourcing
}

SCORING_SYSTEM_PROMPT = """You are a job classifier for an async-first software agency.

Analyze the job and output ONLY valid JSON (no markdown, no explanation):
{
  "meeting_risk": <1-10>,
  "scope_clarity": <1-10>,
  "agency_fit": <1-10>,
  "red_flags": ["list of concerns"],
  "meeting_indicators": ["quotes about meetings/calls from description"]
}

Scoring guide:
- meeting_risk: 10=fully async/no meetings, 1=constant meetings required
  - HIGH RISK (1-3): "daily standup", "video call required", "must overlap hours", "real-time collaboration", "scrum", "agile ceremonies"
  - MEDIUM (4-6): "weekly sync", "occasional calls", "available for meetings"
  - LOW RISK (7-10): "async", "text-based", "flexible timezone", "no meetings", "autonomous"

- scope_clarity: 10=crystal clear deliverables, 1=vague requirements
  - CLEAR (7-10): specific tech stack, defined endpoints, wireframes mentioned
  - VAGUE (1-4): "build my app", "I have an idea", "need a developer"

- agency_fit: 10=perfect for outsourcing, 1=needs embedded team member
  - GOOD FIT (7-10): defined project, clear handoff points, documentation expected
  - BAD FIT (1-4): "part of our team", "long-term relationship", "learn our codebase"

Output ONLY the JSON object, nothing else."""

SCORING_USER_PROMPT = """TITLE: {title}

DESCRIPTION:
{description}"""


from utils.db import get_unanalyzed_jobs, update_job_analysis, update_job_score

# Config file path
CONFIG_PATH = Path(__file__).parent.parent / 'config.toml'


def load_config() -> dict:
    """Load config.toml file."""
    if CONFIG_PATH.exists():
        return toml.load(CONFIG_PATH)
    return {}


def get_api_key() -> str:
    """Get OpenRouter API key from env or config."""
    key = os.environ.get('OPENROUTER_API_KEY')
    if key:
        return key

    config = load_config()
    key = config.get('AI', {}).get('openrouter_api_key', '')
    if key:
        return key

    raise ValueError(
        'OPENROUTER_API_KEY not found. Set env var or add to config.toml [AI] section.'
    )


def get_model(config_key: str, default: str) -> str:
    """Get model name from config or use default."""
    config = load_config()
    return config.get('AI', {}).get(config_key, default)


def call_openrouter(
    messages: list[dict],
    model: str = None,
    temperature: float = 0,
    max_retries: int = 3,
) -> str:
    """
    Call OpenRouter API with retry logic.

    Args:
        messages: List of message dicts with 'role' and 'content'
        model: Model to use (defaults to scoring_model from config)
        temperature: Temperature for generation (0 for deterministic)
        max_retries: Number of retries on failure

    Returns:
        Response content as string
    """
    if model is None:
        model = get_model('scoring_model', 'google/gemini-2.0-flash-exp:free')

    api_key = get_api_key()

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://github.com/utof/Upwork-Job-Scraper',
    }

    payload = {
        'model': model,
        'messages': messages,
        'temperature': temperature,
    }

    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=60.0) as client:
                response = client.post(
                    'https://openrouter.ai/api/v1/chat/completions',
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
                return data['choices'][0]['message']['content']

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = 2 ** (attempt + 1)
                print(f'Rate limited, waiting {wait_time}s...')
                time.sleep(wait_time)
                continue
            raise

        except (httpx.RequestError, KeyError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f'Request failed: {e}, retrying in {wait_time}s...')
                time.sleep(wait_time)
                continue
            raise

    raise RuntimeError(f'Failed after {max_retries} retries')


def parse_ai_response(response: str) -> dict:
    """Parse AI response, handling potential markdown wrapping."""
    text = response.strip()

    # Strip markdown code blocks if present
    if text.startswith('```'):
        lines = text.split('\n')
        # Remove first line (```json or ```)
        lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        text = '\n'.join(lines)

    return json.loads(text)


def calculate_score(analysis: dict) -> float:
    """Calculate weighted score from analysis subscores."""
    score = 0.0
    for key, weight in SCORING_WEIGHTS.items():
        subscore = analysis.get(key, 5)  # Default to 5 if missing
        score += subscore * weight
    return round(score, 2)


def score_job(job: dict) -> tuple[float, dict]:
    """
    Score a single job using AI.

    Args:
        job: Job dict with 'title' and 'description'

    Returns:
        Tuple of (score, analysis_dict)
    """
    title = job.get('title', 'No title')
    description = job.get('description', 'No description')

    messages = [
        {'role': 'system', 'content': SCORING_SYSTEM_PROMPT},
        {
            'role': 'user',
            'content': SCORING_USER_PROMPT.format(title=title, description=description),
        },
    ]

    response = call_openrouter(messages)
    analysis = parse_ai_response(response)

    # Calculate weighted score
    score = calculate_score(analysis)

    return score, analysis


def score_unanalyzed_jobs(limit: int = 50, verbose: bool = True) -> list[dict]:
    """
    Score all unanalyzed jobs in the database.

    Args:
        limit: Maximum number of jobs to process
        verbose: Print progress

    Returns:
        List of dicts with job_id, title, score, and analysis
    """
    # Check API key upfront before fetching jobs
    try:
        get_api_key()
    except ValueError as e:
        print(f'ERROR: {e}')
        return []

    jobs = get_unanalyzed_jobs(limit=limit)

    if not jobs:
        if verbose:
            print('No unanalyzed jobs found.')
        return []

    if verbose:
        print(f'Scoring {len(jobs)} jobs...')

    results = []
    for i, job in enumerate(jobs):
        job_id = job['job_id']
        title = job.get('title', 'No title')

        try:
            score, analysis = score_job(job)

            # Store in database
            update_job_score(job_id, score)
            update_job_analysis(job_id, json.dumps(analysis))

            results.append({
                'job_id': job_id,
                'title': title,
                'score': score,
                'analysis': analysis,
            })

            if verbose:
                meeting_risk = analysis.get('meeting_risk', '?')
                print(f'  [{i + 1}/{len(jobs)}] {score:.1f} (mtg:{meeting_risk}) - {title[:50]}')

            # Small delay to be nice to free API tier
            time.sleep(0.5)

        except Exception as e:
            print(f'  [{i + 1}/{len(jobs)}] ERROR: {e} - {title[:50]}')
            continue

    if verbose:
        scored_count = len(results)
        avg_score = sum(r['score'] for r in results) / scored_count if scored_count else 0
        print(f'\nScored {scored_count}/{len(jobs)} jobs. Average score: {avg_score:.1f}')

    return results
