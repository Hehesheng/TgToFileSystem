#!/usr/bin/env python3
"""
msg_js Compact Migration Script

Migrates old msg_js format to compact format for storage optimization.
Expected savings: ~86% reduction in msg_js size.

Usage:
    python backend/script/migrate_msg_js.py              # Full migration
    python backend/script/migrate_msg_js.py --limit 1000 # Limited test run
    python backend/script/migrate_msg_js.py --vacuum     # Full migration + VACUUM
    python backend/script/migrate_msg_js.py --start-date 1700000000  # Resume from date

Progress file: backend/db/migration_progress.json (stores last_processed_date)
"""

import argparse
import sys
import os
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.UserManager import UserManager

PROGRESS_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'migration_progress.json')


def estimate_old_count(db, sample_size=5000) -> int:
    """Estimate count of old format records using sampling."""
    rows = db.cur.execute(
        "SELECT msg_js FROM message LIMIT ?", (sample_size,)
    ).fetchall()
    old_count = sum(1 for r in rows if not db.is_compact_format(r[0]))
    total = db.cur.execute("SELECT COUNT(*) FROM message").fetchone()[0]
    return int(old_count / sample_size * total)


def load_progress() -> dict:
    """Load migration progress from file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_progress(data: dict):
    """Save migration progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Migrate msg_js to compact format')
    parser.add_argument('--limit', type=int, default=None, help='Maximum records to migrate (None = all)')
    parser.add_argument('--vacuum', action='store_true', help='Run VACUUM after migration')
    parser.add_argument('--vacuum-only', action='store_true', help='Only run VACUUM, skip migration')
    parser.add_argument('--stats', action='store_true', help='Only show stats, no migration')
    parser.add_argument('--fetch-batch', type=int, default=100, help='Records to fetch per batch')
    parser.add_argument('--start-date', type=int, default=None, help='Start from this date_time (resume)')
    parser.add_argument('--resume', action='store_true', help='Resume from last saved progress')
    args = parser.parse_args()

    db = UserManager()

    # Show current stats
    print('=== Current Storage Stats ===')
    stats = db.get_storage_stats()
    for k, v in stats.items():
        print(f'  {k}: {v}')

    if args.stats:
        progress = load_progress()
        if progress:
            print(f'\n=== Saved Progress ===')
            print(f'  last_processed_date: {progress.get("last_processed_date")}')
            print(f'  migrated: {progress.get("migrated")}')
        return

    # Vacuum only mode
    if args.vacuum_only:
        print('\n=== Running VACUUM Only ===')
        print('This may take a few minutes...')
        db.cur.execute('VACUUM')
        db.con.commit()
        print('VACUUM completed.')

        # Show final stats
        print('\n=== Final Storage Stats ===')
        stats = db.get_storage_stats()
        for k, v in stats.items():
            print(f'  {k}: {v}')

        # Show file size
        db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'user.db')
        if os.path.exists(db_path):
            file_size_mb = os.path.getsize(db_path) / 1024 / 1024
            print(f'  db_file_size_mb: {file_size_mb:.2f}')
        return

    # Determine start_date
    start_date = args.start_date
    if args.resume:
        progress = load_progress()
        start_date = progress.get('last_processed_date')
        if start_date:
            print(f'\nResuming from date: {start_date}')
        else:
            print('\nNo saved progress found, starting from beginning')

    # Estimate records to migrate
    estimated_old = estimate_old_count(db)
    to_migrate = estimated_old
    if args.limit:
        to_migrate = min(to_migrate, args.limit)

    if to_migrate == 0:
        print('\nNo records need migration.')
        return

    print(f'\nEstimated {estimated_old} old format records to migrate')

    # Confirm before full migration
    if args.limit is None and estimated_old > 10000:
        print(f'WARNING: About to migrate ~{estimated_old} records')
        print('This may take several minutes. Continue? [y/N]')
        response = input().strip().lower()
        if response != 'y':
            print('Cancelled.')
            return

    # Run migration with progress display
    print('\n=== Starting Migration ===')
    print(f'Total records: {stats.get("total_count", "N/A")}')
    print('Interrupt with Ctrl+C, resume with --resume\n')

    def save_batch_progress(checked, migrated, errors, last_date):
        """Save progress after each batch."""
        save_progress({
            'last_processed_date': last_date,
            'migrated': migrated,
            'total_checked': checked,
            'errors': errors,
        })

    result = db.migrate_to_compact(
        limit=args.limit,
        progress_callback=None,
        fetch_batch_size=args.fetch_batch,
        start_date=start_date,
        batch_callback=save_batch_progress,
    )

    print(f'\nMigration Result:')
    for k, v in result.items():
        print(f'  {k}: {v}')

    # Save progress for resume
    if result.get('last_processed_date'):
        save_progress({
            'last_processed_date': result['last_processed_date'],
            'migrated': result.get('migrated', 0),
            'total_checked': result.get('total_checked', 0),
        })
        print(f'\nProgress saved to {PROGRESS_FILE}')

    # VACUUM if requested and migration complete
    if args.vacuum and not result.get('limit_reached'):
        print('\n=== Running VACUUM ===')
        print('This may take a few minutes...')
        db.cur.execute('VACUUM')
        db.con.commit()
        print('VACUUM completed.')
        # Remove progress file after complete migration + vacuum
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print('Progress file removed.')

    # Show final stats
    print('\n=== Final Storage Stats ===')
    stats = db.get_storage_stats()
    for k, v in stats.items():
        print(f'  {k}: {v}')

    # Show file size
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'user.db')
    if os.path.exists(db_path):
        file_size_mb = os.path.getsize(db_path) / 1024 / 1024
        print(f'  db_file_size_mb: {file_size_mb:.2f}')


if __name__ == '__main__':
    main()