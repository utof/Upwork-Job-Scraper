#!/usr/bin/env python
"""
CLI script for AI-powered job scoring.

Usage:
    uv run score_jobs.py              # Score all unanalyzed jobs
    uv run score_jobs.py --limit 10   # Score up to 10 jobs
    uv run score_jobs.py --stats      # Show scoring statistics
"""

import argparse
import json

from utils.ai_scorer import score_unanalyzed_jobs
from utils.db import get_connection, get_job_count


def show_stats():
    """Display scoring statistics."""
    with get_connection() as conn:
        # Total jobs
        total = conn.execute('SELECT COUNT(*) FROM jobs').fetchone()[0]

        # Scored jobs
        scored = conn.execute('SELECT COUNT(*) FROM jobs WHERE score IS NOT NULL').fetchone()[0]

        # Unscored jobs
        unscored = total - scored

        # Score distribution
        score_ranges = [
            ('High (8-10)', 8, 10),
            ('Medium (5-7.9)', 5, 7.99),
            ('Low (1-4.9)', 1, 4.99),
        ]

        print(f'\n=== Job Scoring Statistics ===')
        print(f'Total jobs: {total}')
        print(f'Scored: {scored}')
        print(f'Unscored: {unscored}')

        if scored > 0:
            # Average score
            avg = conn.execute('SELECT AVG(score) FROM jobs WHERE score IS NOT NULL').fetchone()[0]
            print(f'\nAverage score: {avg:.2f}')

            print('\nScore distribution:')
            for label, low, high in score_ranges:
                count = conn.execute(
                    'SELECT COUNT(*) FROM jobs WHERE score >= ? AND score <= ?',
                    (low, high),
                ).fetchone()[0]
                pct = (count / scored * 100) if scored else 0
                print(f'  {label}: {count} ({pct:.1f}%)')

            # Top 5 jobs
            print('\nTop 5 jobs:')
            rows = conn.execute(
                '''SELECT title, score, json_extract(ai_analysis, '$.meeting_risk') as mtg
                   FROM jobs WHERE score IS NOT NULL
                   ORDER BY score DESC LIMIT 5'''
            ).fetchall()
            for row in rows:
                print(f'  {row[1]:.1f} (mtg:{row[2]}) - {row[0][:50]}')

            # Meeting risk analysis
            print('\nMeeting risk breakdown:')
            for label, low, high in [('Low risk (7-10)', 7, 10), ('Medium (4-6)', 4, 6), ('High risk (1-3)', 1, 3)]:
                count = conn.execute(
                    '''SELECT COUNT(*) FROM jobs
                       WHERE json_extract(ai_analysis, '$.meeting_risk') >= ?
                       AND json_extract(ai_analysis, '$.meeting_risk') <= ?''',
                    (low, high),
                ).fetchone()[0]
                print(f'  {label}: {count}')


def main():
    parser = argparse.ArgumentParser(description='AI-powered job scoring')
    parser.add_argument('--limit', type=int, default=50, help='Max jobs to score (default: 50)')
    parser.add_argument('--stats', action='store_true', help='Show scoring statistics')
    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    # Run scoring
    results = score_unanalyzed_jobs(limit=args.limit, verbose=True)

    if results:
        # Show quick summary of high-scoring jobs
        high_scoring = [r for r in results if r['score'] >= 8.0]
        if high_scoring:
            print(f'\n=== High-scoring jobs (>= 8.0) ===')
            for r in high_scoring:
                mtg = r['analysis'].get('meeting_risk', '?')
                flags = r['analysis'].get('red_flags', [])
                print(f"  {r['score']:.1f} - {r['title'][:60]}")
                if flags:
                    print(f"       Red flags: {', '.join(flags[:2])}")


if __name__ == '__main__':
    main()
