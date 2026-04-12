#!/usr/bin/env python3
"""
msg_js Compact Migration Script

Migrates old msg_js format to compact format for storage optimization.
Expected savings: ~86% reduction in msg_js size.

Usage:
    python backend/script/migrate_msg_js.py              # Full migration
    python backend/script/migrate_msg_js.py --limit 1000 # Limited test run
    python backend/script/migrate_msg_js.py --vacuum     # Full migration + VACUUM
"""

import argparse
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.UserManager import UserManager


def main():
    parser = argparse.ArgumentParser(description='Migrate msg_js to compact format')
    parser.add_argument('--limit', type=int, default=None, help='Maximum records to migrate (None = all)')
    parser.add_argument('--batch', type=int, default=1000, help='Batch size for commits')
    parser.add_argument('--vacuum', action='store_true', help='Run VACUUM after migration')
    parser.add_argument('--stats', action='store_true', help='Only show stats, no migration')
    args = parser.parse_args()

    db = UserManager()

    # Show current stats
    print('=== Current Storage Stats ===')
    stats = db.get_storage_stats()
    for k, v in stats.items():
        print(f'  {k}: {v}')

    if args.stats:
        return

    # Confirm before full migration
    if args.limit is None and stats.get('old_count_estimate', 0) > 10000:
        print(f'\nWARNING: About to migrate ~{stats["old_count_estimate"]} records')
        print('This may take several minutes. Continue? [y/N]')
        response = input().strip().lower()
        if response != 'y':
            print('Cancelled.')
            return

    # Run migration
    print('\n=== Starting Migration ===')
    result = db.migrate_to_compact(batch_size=args.batch, limit=args.limit)
    print(f'\nMigration Result:')
    for k, v in result.items():
        print(f'  {k}: {v}')

    # VACUUM if requested
    if args.vacuum:
        print('\n=== Running VACUUM ===')
        print('This may take a few minutes...')
        db.cur.execute('VACUUM')
        db.con.commit()
        print('VACUUM completed.')

    # Show final stats
    print('\n=== Final Storage Stats ===')
    stats = db.get_storage_stats()
    for k, v in stats.items():
        print(f'  {k}: {v}')

    # Estimate file size
    db_path = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/db/user.db'
    if os.path.exists(db_path):
        file_size_mb = os.path.getsize(db_path) / 1024 / 1024
        print(f'  db_file_size_mb: {file_size_mb:.2f}')


if __name__ == '__main__':
    main()