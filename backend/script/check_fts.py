#!/usr/bin/env python3
"""
FTS Health Check Script

Checks FTS table status and provides rebuild option.

Usage:
    python backend/script/check_fts.py          # Check status
    python backend/script/check_fts.py --rebuild # Rebuild FTS index
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.UserManager import UserManager


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Check or rebuild FTS index')
    parser.add_argument('--rebuild', action='store_true', help='Rebuild FTS index')
    args = parser.parse_args()

    db = UserManager()

    # Check if FTS table exists
    cur = db.cur
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [t[0] for t in tables]

    print('=== FTS Health Check ===')
    print(f'\nExisting tables: {table_names}')

    if 'message_fts' not in table_names:
        print('\nWARNING: message_fts table NOT FOUND!')
        print('This table will be auto-created on next UserManager init.')
        print('But sync may be slow with large data.')
        return

    # Check counts
    msg_count = cur.execute('SELECT COUNT(*) FROM message').fetchone()[0]
    fts_count = cur.execute('SELECT COUNT(*) FROM message_fts').fetchone()[0]

    print(f'\nCounts:')
    print(f'  message table: {msg_count}')
    print(f'  message_fts table: {fts_count}')
    print(f'  sync status: {"OK" if msg_count == fts_count else "MISMATCH"}')

    if msg_count != fts_count:
        print(f'\nWARNING: {msg_count - fts_count} records not in FTS!')
        print('Search results may be incomplete.')

    # Test search
    print('\n=== Search Test ===')
    try:
        # Simple test query
        test_keyword = 'test'
        fts_exists = cur.execute(
            "SELECT COUNT(*) FROM message_fts WHERE message_fts MATCH ?",
            (f'"test"',)
        ).fetchone()[0]
        print(f'  Test query returned: {fts_exists} results')
        if fts_exists > 0:
            print('  FTS search: OK')
        else:
            print('  FTS search: No results (may be empty index)')
    except Exception as err:
        print(f'  FTS search error: {err}')
        print('  FTS may not be working properly!')

    # Rebuild if requested
    if args.rebuild:
        print('\n=== Rebuilding FTS ===')
        result = db.rebuild_fts()
        if result:
            print('FTS rebuilt successfully.')
        else:
            print('FTS rebuild failed.')

        # Verify
        fts_count = cur.execute('SELECT COUNT(*) FROM message_fts').fetchone()[0]
        print(f'New FTS count: {fts_count}')
        print(f'Sync status: {"OK" if msg_count == fts_count else "MISMATCH"}')


if __name__ == '__main__':
    main()