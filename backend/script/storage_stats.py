#!/usr/bin/env python3
"""
Storage Statistics Script

Shows current storage statistics and potential savings.

Usage:
    python backend/script/storage_stats.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.UserManager import UserManager


def main():
    db = UserManager()

    print('=== Storage Statistics ===')
    stats = db.get_storage_stats()

    print(f'\nMessage Table:')
    print(f'  Total messages: {stats.get("total_count", 0)}')
    print(f'  Old format estimate: {stats.get("old_count_estimate", 0)}')
    print(f'  Compact format estimate: {stats.get("compact_count_estimate", 0)}')

    print(f'\nSize Analysis:')
    print(f'  Avg old msg_js: {stats.get("avg_old_size", 0):.0f} chars')
    print(f'  Avg compact msg_js: {stats.get("avg_compact_size", 0):.0f} chars')

    db_path = f'{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/db/user.db'
    if os.path.exists(db_path):
        file_size_mb = os.path.getsize(db_path) / 1024 / 1024
        print(f'  Current DB file: {file_size_mb:.2f} MB')

    potential_saving = stats.get('potential_saving_mb', 0)
    if potential_saving > 0:
        print(f'\nPotential Savings:')
        print(f'  Estimated saving: {potential_saving:.0f} MB')
        print(f'  Estimated final size: {file_size_mb - potential_saving:.0f} MB')
        print(f'  Compression ratio: {(potential_saving / file_size_mb * 100):.1f}%')

    print(f'\nFTS Table:')
    fts_stats = db.get_fts_stats()
    print(f'  FTS count: {fts_stats.get("fts_count", 0)}')
    print(f'  Sync status: {fts_stats.get("sync_status", False)}')


if __name__ == '__main__':
    main()