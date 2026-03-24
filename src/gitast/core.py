"""Core git mining and data storage for GitAST"""
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple


def parse_date_filter(value: str) -> int:
    """Parse a date filter string and return a unix timestamp.

    Accepts ISO dates (2026-01-01) and relative durations (30d, 6m, 1y).
    """
    # Try relative format first: 30d, 6m, 1y
    match = re.match(r'^(\d+)([dmy])$', value.strip())
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        now = int(time.time())
        if unit == 'd':
            return now - amount * 86400
        elif unit == 'm':
            return now - amount * 30 * 86400
        elif unit == 'y':
            return now - amount * 365 * 86400

    # Try ISO date format
    try:
        dt = datetime.fromisoformat(value.strip())
        return int(dt.timestamp())
    except ValueError:
        raise ValueError(f"Cannot parse date filter: {value!r}. Use ISO (2026-01-01) or relative (30d, 6m, 1y).")

import git
from git import Repo

from .models import GitCommit, FunctionInfo, BlameEntry, FunctionChange


class DataStore:
    """SQLite storage for GitAST index data."""

    SCHEMA_VERSION = 1

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self) -> None:
        if self.conn is not None:
            return
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        import re as _re
        self.conn.create_function('REGEXP', 2, lambda pattern, text: bool(_re.search(pattern, text or '')))

    def create_schema(self) -> None:
        c = self.conn
        c.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS commits (
                hash TEXT PRIMARY KEY,
                author TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                message TEXT,
                files_changed INTEGER DEFAULT 0,
                semantic_tags TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS functions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                language TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                kind TEXT NOT NULL,
                signature TEXT DEFAULT '',
                docstring TEXT DEFAULT '',
                UNIQUE(name, file_path, kind, start_line)
            );

            CREATE TABLE IF NOT EXISTS function_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                function_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                change_type TEXT NOT NULL,
                lines_added INTEGER DEFAULT 0,
                lines_removed INTEGER DEFAULT 0,
                author TEXT DEFAULT '',
                timestamp INTEGER,
                message TEXT DEFAULT '',
                FOREIGN KEY (commit_hash) REFERENCES commits(hash)
            );

            CREATE TABLE IF NOT EXISTS blame_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                function_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                author TEXT NOT NULL,
                line_count INTEGER NOT NULL,
                percentage REAL NOT NULL,
                commit_hash TEXT DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_functions_file ON functions(file_path);
            CREATE INDEX IF NOT EXISTS idx_functions_name ON functions(name);
            CREATE INDEX IF NOT EXISTS idx_fchanges_func ON function_changes(function_name, file_path);
            CREATE INDEX IF NOT EXISTS idx_fchanges_commit ON function_changes(commit_hash);
            CREATE INDEX IF NOT EXISTS idx_blame_func ON blame_entries(function_name, file_path);

            CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
                type,
                name,
                file_path,
                author,
                content,
                detail
            );

            CREATE TABLE IF NOT EXISTS embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                ref_id TEXT NOT NULL,
                text TEXT NOT NULL,
                vector BLOB NOT NULL,
                model TEXT NOT NULL,
                UNIQUE(type, ref_id)
            );

            CREATE INDEX IF NOT EXISTS idx_embeddings_type ON embeddings(type);

            CREATE TABLE IF NOT EXISTS function_renames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commit_hash TEXT NOT NULL,
                old_name TEXT NOT NULL,
                old_file_path TEXT NOT NULL,
                old_kind TEXT NOT NULL,
                new_name TEXT NOT NULL,
                new_file_path TEXT NOT NULL,
                new_kind TEXT NOT NULL,
                confidence REAL NOT NULL,
                signals TEXT DEFAULT '',
                FOREIGN KEY (commit_hash) REFERENCES commits(hash)
            );
            CREATE INDEX IF NOT EXISTS idx_renames_old ON function_renames(old_name, old_file_path);
            CREATE INDEX IF NOT EXISTS idx_renames_new ON function_renames(new_name, new_file_path);

            CREATE TABLE IF NOT EXISTS config_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                key_path TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                author TEXT DEFAULT '',
                timestamp INTEGER,
                message TEXT DEFAULT '',
                FOREIGN KEY (commit_hash) REFERENCES commits(hash)
            );
            CREATE INDEX IF NOT EXISTS idx_config_file ON config_changes(file_path);
            CREATE INDEX IF NOT EXISTS idx_config_key ON config_changes(key_path);
            CREATE INDEX IF NOT EXISTS idx_config_commit ON config_changes(commit_hash);

            CREATE TABLE IF NOT EXISTS dep_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                package TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_version TEXT,
                new_version TEXT,
                author TEXT DEFAULT '',
                timestamp INTEGER,
                message TEXT DEFAULT '',
                FOREIGN KEY (commit_hash) REFERENCES commits(hash)
            );
            CREATE INDEX IF NOT EXISTS idx_dep_file ON dep_changes(file_path);
            CREATE INDEX IF NOT EXISTS idx_dep_pkg ON dep_changes(package);
            CREATE INDEX IF NOT EXISTS idx_dep_commit ON dep_changes(commit_hash);
        """)
        c.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                  ("schema_version", str(self.SCHEMA_VERSION)))
        c.commit()
        # Migrate existing databases: add docstring column if missing
        try:
            c.execute("SELECT docstring FROM functions LIMIT 1")
        except sqlite3.OperationalError:
            c.execute("ALTER TABLE functions ADD COLUMN docstring TEXT DEFAULT ''")
            c.commit()
        # Migrate: add config_changes table if missing
        try:
            c.execute("SELECT id FROM config_changes LIMIT 1")
        except sqlite3.OperationalError:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS config_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT NOT NULL,
                    key_path TEXT NOT NULL,
                    commit_hash TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    author TEXT DEFAULT '',
                    timestamp INTEGER,
                    message TEXT DEFAULT '',
                    FOREIGN KEY (commit_hash) REFERENCES commits(hash)
                );
                CREATE INDEX IF NOT EXISTS idx_config_file ON config_changes(file_path);
                CREATE INDEX IF NOT EXISTS idx_config_key ON config_changes(key_path);
                CREATE INDEX IF NOT EXISTS idx_config_commit ON config_changes(commit_hash);
            """)
        # Migrate: add dep_changes table if missing
        try:
            c.execute("SELECT id FROM dep_changes LIMIT 1")
        except sqlite3.OperationalError:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS dep_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT NOT NULL,
                    package TEXT NOT NULL,
                    commit_hash TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    old_version TEXT,
                    new_version TEXT,
                    author TEXT DEFAULT '',
                    timestamp INTEGER,
                    message TEXT DEFAULT '',
                    FOREIGN KEY (commit_hash) REFERENCES commits(hash)
                );
                CREATE INDEX IF NOT EXISTS idx_dep_file ON dep_changes(file_path);
                CREATE INDEX IF NOT EXISTS idx_dep_pkg ON dep_changes(package);
                CREATE INDEX IF NOT EXISTS idx_dep_commit ON dep_changes(commit_hash);
            """)

    def clear_all(self) -> None:
        """Clear all indexed data for a fresh re-index."""
        for table in ['search_index', 'embeddings', 'function_renames', 'dep_changes', 'config_changes', 'blame_entries', 'function_changes', 'functions', 'commits']:
            self.conn.execute(f"DELETE FROM {table}")
        self.conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        """Read a value from the meta table."""
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row['value'] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Write a value to the meta table."""
        self.conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
        self.conn.commit()

    def get_indexed_commit_hashes(self) -> set:
        """Return set of all commit hashes already in the commits table."""
        rows = self.conn.execute("SELECT hash FROM commits").fetchall()
        return {r['hash'] for r in rows}

    def delete_file_data(self, file_path: str) -> None:
        """Delete functions and blame_entries for a specific file."""
        self.conn.execute("DELETE FROM functions WHERE file_path = ?", (file_path,))
        self.conn.execute("DELETE FROM blame_entries WHERE file_path = ?", (file_path,))

    def save_commit(self, commit: GitCommit) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO commits (hash, author, timestamp, message, files_changed, semantic_tags) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (commit.hash, commit.author, int(commit.timestamp.timestamp()),
             commit.message, commit.files_changed, ','.join(commit.semantic_tags))
        )

    def save_function(self, func: FunctionInfo) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO functions (name, file_path, language, start_line, end_line, kind, signature, docstring) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (func.name, func.file_path, func.language, func.start_line, func.end_line,
             func.kind, func.signature, func.docstring)
        )

    def save_function_change(self, change: FunctionChange) -> None:
        ts = int(change.timestamp.timestamp()) if change.timestamp else 0
        self.conn.execute(
            "INSERT INTO function_changes (function_name, file_path, commit_hash, change_type, "
            "lines_added, lines_removed, author, timestamp, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (change.function_name, change.file_path, change.commit_hash, change.change_type,
             change.lines_added, change.lines_removed, change.author, ts, change.message)
        )

    def save_blame_entry(self, entry: BlameEntry) -> None:
        self.conn.execute(
            "INSERT INTO blame_entries (function_name, file_path, author, line_count, percentage, commit_hash) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (entry.function_name, entry.file_path, entry.author, entry.line_count,
             entry.percentage, entry.commit_hash)
        )

    def save_function_rename(self, commit_hash: str, old_name: str, old_file_path: str,
                              old_kind: str, new_name: str, new_file_path: str,
                              new_kind: str, confidence: float, signals: str = "") -> None:
        """Record a detected function rename/move."""
        self.conn.execute(
            "INSERT INTO function_renames (commit_hash, old_name, old_file_path, old_kind, "
            "new_name, new_file_path, new_kind, confidence, signals) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (commit_hash, old_name, old_file_path, old_kind,
             new_name, new_file_path, new_kind, confidence, signals)
        )

    def save_config_change(self, change) -> None:
        """Save a config key-path change."""
        from .models import ConfigChange
        ts = int(change.timestamp.timestamp()) if change.timestamp else 0
        self.conn.execute(
            "INSERT INTO config_changes (file_path, key_path, commit_hash, change_type, "
            "old_value, new_value, author, timestamp, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (change.file_path, change.key_path, change.commit_hash, change.change_type,
             change.old_value, change.new_value, change.author, ts, change.message)
        )

    def get_config_history(self, key_path: str, file_path: Optional[str] = None,
                           limit: int = 50) -> List[Dict]:
        """Get change history for a config key path."""
        if file_path:
            rows = self.conn.execute(
                """
                SELECT * FROM config_changes
                WHERE key_path LIKE ? AND file_path LIKE ?
                ORDER BY timestamp DESC LIMIT ?
                """,
                (f"%{key_path}%", f"%{file_path}%", limit)
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM config_changes
                WHERE key_path LIKE ?
                ORDER BY timestamp DESC LIMIT ?
                """,
                (f"%{key_path}%", limit)
            ).fetchall()
        return [
            {
                'file_path': r['file_path'],
                'key_path': r['key_path'],
                'change_type': r['change_type'],
                'old_value': r['old_value'],
                'new_value': r['new_value'],
                'author': r['author'],
                'commit_hash': r['commit_hash'],
                'timestamp': r['timestamp'],
            }
            for r in rows
        ]

    def get_config_keys(self, file_filter: Optional[str] = None,
                        limit: int = 100) -> List[Dict]:
        """List tracked config keys with change counts."""
        if file_filter:
            rows = self.conn.execute(
                """
                SELECT key_path, file_path, COUNT(*) AS change_count,
                       MAX(timestamp) AS last_changed
                FROM config_changes
                WHERE file_path LIKE ?
                GROUP BY key_path, file_path
                ORDER BY change_count DESC LIMIT ?
                """,
                (f"%{file_filter}%", limit)
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT key_path, file_path, COUNT(*) AS change_count,
                       MAX(timestamp) AS last_changed
                FROM config_changes
                GROUP BY key_path, file_path
                ORDER BY change_count DESC LIMIT ?
                """,
                (limit,)
            ).fetchall()
        return [
            {
                'key_path': r['key_path'],
                'file_path': r['file_path'],
                'change_count': r['change_count'],
                'last_changed': r['last_changed'],
            }
            for r in rows
        ]

    def save_dep_change(self, change) -> None:
        """Save a dependency change."""
        ts = int(change.timestamp.timestamp()) if change.timestamp else 0
        self.conn.execute(
            "INSERT INTO dep_changes (file_path, package, commit_hash, change_type, "
            "old_version, new_version, author, timestamp, message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (change.file_path, change.package, change.commit_hash, change.change_type,
             change.old_version, change.new_version, change.author, ts, change.message)
        )

    def get_dep_history(self, package: Optional[str] = None, file_filter: Optional[str] = None,
                        change_type: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """Get dependency change history."""
        conditions = []
        params = []

        if package:
            conditions.append("package LIKE ?")
            params.append(f"%{package}%")
        if file_filter:
            conditions.append("file_path LIKE ?")
            params.append(f"%{file_filter}%")
        if change_type:
            conditions.append("change_type = ?")
            params.append(change_type)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        rows = self.conn.execute(
            f"SELECT * FROM dep_changes {where} ORDER BY timestamp DESC LIMIT ?",
            params
        ).fetchall()

        return [
            {
                'file_path': r['file_path'],
                'package': r['package'],
                'change_type': r['change_type'],
                'old_version': r['old_version'],
                'new_version': r['new_version'],
                'author': r['author'],
                'commit_hash': r['commit_hash'],
                'timestamp': r['timestamp'],
            }
            for r in rows
        ]

    def get_dep_summary(self, limit: int = 100) -> List[Dict]:
        """List tracked packages with change counts."""
        rows = self.conn.execute(
            """
            SELECT package, file_path, COUNT(*) AS change_count,
                   MAX(timestamp) AS last_changed,
                   SUM(CASE WHEN change_type = 'bumped' THEN 1 ELSE 0 END) AS bumps,
                   SUM(CASE WHEN change_type = 'added' THEN 1 ELSE 0 END) AS adds,
                   SUM(CASE WHEN change_type = 'removed' THEN 1 ELSE 0 END) AS removes
            FROM dep_changes
            GROUP BY package, file_path
            ORDER BY change_count DESC LIMIT ?
            """,
            (limit,)
        ).fetchall()
        return [
            {
                'package': r['package'],
                'file_path': r['file_path'],
                'change_count': r['change_count'],
                'last_changed': r['last_changed'],
                'bumps': r['bumps'],
                'adds': r['adds'],
                'removes': r['removes'],
            }
            for r in rows
        ]

    def flush(self) -> None:
        if self.conn:
            self.conn.commit()

    def _follow_rename_chain(self, function_name: str, file_path: str) -> List[Tuple[str, str]]:
        """Follow rename chain backwards, returning list of (name, file_path) pairs."""
        visited = set()
        pairs = [(function_name, file_path)]
        visited.add((function_name, file_path))
        # Follow backwards: find old names that were renamed TO this
        queue = [(function_name, file_path)]
        while queue:
            name, fp = queue.pop(0)
            rows = self.conn.execute(
                "SELECT old_name, old_file_path FROM function_renames WHERE new_name = ? AND new_file_path = ?",
                (name, fp)
            ).fetchall()
            for r in rows:
                pair = (r['old_name'], r['old_file_path'])
                if pair not in visited:
                    visited.add(pair)
                    pairs.append(pair)
                    queue.append(pair)
        # Follow forwards: find new names that this was renamed TO
        queue = [(function_name, file_path)]
        while queue:
            name, fp = queue.pop(0)
            rows = self.conn.execute(
                "SELECT new_name, new_file_path FROM function_renames WHERE old_name = ? AND old_file_path = ?",
                (name, fp)
            ).fetchall()
            for r in rows:
                pair = (r['new_name'], r['new_file_path'])
                if pair not in visited:
                    visited.add(pair)
                    pairs.append(pair)
                    queue.append(pair)
        return pairs

    def get_function_history(self, file_path: str, function_name: str,
                             fuzzy_path: bool = False) -> List[FunctionChange]:
        if fuzzy_path:
            rows = self.conn.execute(
                "SELECT * FROM function_changes WHERE file_path LIKE ? AND function_name = ? "
                "ORDER BY timestamp DESC",
                (f"%{file_path}%", function_name)
            ).fetchall()
        else:
            # Collect history across rename chain
            all_rows = []
            seen_ids = set()
            pairs = self._follow_rename_chain(function_name, file_path)
            for name, fp in pairs:
                rows = self.conn.execute(
                    "SELECT * FROM function_changes WHERE file_path = ? AND function_name = ? "
                    "ORDER BY timestamp DESC",
                    (fp, name)
                ).fetchall()
                for r in rows:
                    rid = (r['commit_hash'], r['function_name'], r['file_path'])
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_rows.append(r)
            rows = sorted(all_rows, key=lambda r: r['timestamp'] or 0, reverse=True)
        return [
            FunctionChange(
                function_name=r['function_name'], file_path=r['file_path'],
                commit_hash=r['commit_hash'], change_type=r['change_type'],
                lines_added=r['lines_added'], lines_removed=r['lines_removed'],
                author=r['author'],
                timestamp=datetime.fromtimestamp(r['timestamp']) if r['timestamp'] else None,
                message=r['message']
            ) for r in rows
        ]

    def get_function_history_by_name(self, function_name: str) -> List[FunctionChange]:
        """Get history for a function by name only (across all files).
        Also follows rename chains for any file paths found."""
        # First get direct matches to find file paths
        direct_rows = self.conn.execute(
            "SELECT DISTINCT file_path FROM function_changes WHERE function_name = ?",
            (function_name,)
        ).fetchall()

        all_rows = []
        seen_ids = set()
        # Collect from rename chains for each file path
        file_paths = [r['file_path'] for r in direct_rows]
        if file_paths:
            for fp in file_paths:
                pairs = self._follow_rename_chain(function_name, fp)
                for name, pfp in pairs:
                    rows = self.conn.execute(
                        "SELECT * FROM function_changes WHERE function_name = ? AND file_path = ?",
                        (name, pfp)
                    ).fetchall()
                    for r in rows:
                        rid = (r['commit_hash'], r['function_name'], r['file_path'])
                        if rid not in seen_ids:
                            seen_ids.add(rid)
                            all_rows.append(r)
        else:
            # Also check if this name appears as old_name in renames
            rename_rows = self.conn.execute(
                "SELECT old_file_path FROM function_renames WHERE old_name = ?",
                (function_name,)
            ).fetchall()
            for rr in rename_rows:
                pairs = self._follow_rename_chain(function_name, rr['old_file_path'])
                for name, pfp in pairs:
                    rows = self.conn.execute(
                        "SELECT * FROM function_changes WHERE function_name = ? AND file_path = ?",
                        (name, pfp)
                    ).fetchall()
                    for r in rows:
                        rid = (r['commit_hash'], r['function_name'], r['file_path'])
                        if rid not in seen_ids:
                            seen_ids.add(rid)
                            all_rows.append(r)

        all_rows.sort(key=lambda r: (r['file_path'], -(r['timestamp'] or 0)))
        return [
            FunctionChange(
                function_name=r['function_name'], file_path=r['file_path'],
                commit_hash=r['commit_hash'], change_type=r['change_type'],
                lines_added=r['lines_added'], lines_removed=r['lines_removed'],
                author=r['author'],
                timestamp=datetime.fromtimestamp(r['timestamp']) if r['timestamp'] else None,
                message=r['message']
            ) for r in all_rows
        ]

    def get_deleted_functions(self, limit: int = 50,
                              pattern: Optional[str] = None) -> List[Dict]:
        """Functions that were deleted and no longer exist at HEAD."""
        where_extra = ""
        params: list = []
        if pattern:
            where_extra = "AND fc.function_name LIKE ?"
            params.append(f"%{pattern}%")
        params.append(limit)
        rows = self.conn.execute(
            f"""
            SELECT DISTINCT fc.function_name, fc.file_path,
                MAX(fc.timestamp) AS deleted_at, fc.author
            FROM function_changes fc
            WHERE fc.change_type = 'deleted'
            AND NOT EXISTS (
                SELECT 1 FROM functions f
                WHERE f.name = fc.function_name AND f.file_path = fc.file_path
            )
            {where_extra}
            GROUP BY fc.function_name, fc.file_path
            ORDER BY deleted_at DESC
            LIMIT ?
            """,
            params
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'deleted_at': datetime.fromtimestamp(r['deleted_at']) if r['deleted_at'] else None,
                'author': r['author'],
            }
            for r in rows
        ]

    def get_function_blame(self, file_path: str, function_name: str,
                           fuzzy_path: bool = False) -> List[BlameEntry]:
        if fuzzy_path:
            rows = self.conn.execute(
                "SELECT * FROM blame_entries WHERE file_path LIKE ? AND function_name = ? "
                "ORDER BY percentage DESC",
                (f"%{file_path}%", function_name)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM blame_entries WHERE file_path = ? AND function_name = ? "
                "ORDER BY percentage DESC",
                (file_path, function_name)
            ).fetchall()
        return [
            BlameEntry(
                function_name=r['function_name'], file_path=r['file_path'],
                author=r['author'], line_count=r['line_count'],
                percentage=r['percentage'], commit_hash=r['commit_hash']
            ) for r in rows
        ]

    def get_functions_in_file(self, file_path: str) -> List[FunctionInfo]:
        rows = self.conn.execute(
            "SELECT * FROM functions WHERE file_path = ? ORDER BY start_line",
            (file_path,)
        ).fetchall()
        return [
            FunctionInfo(
                name=r['name'], file_path=r['file_path'], language=r['language'],
                start_line=r['start_line'], end_line=r['end_line'],
                kind=r['kind'], signature=r['signature']
            ) for r in rows
        ]

    def get_commit(self, hash: str) -> Optional[GitCommit]:
        row = self.conn.execute("SELECT * FROM commits WHERE hash = ?", (hash,)).fetchone()
        if not row:
            return None
        return GitCommit(
            hash=row['hash'], author=row['author'],
            timestamp=datetime.fromtimestamp(row['timestamp']),
            message=row['message'], files_changed=row['files_changed'],
            semantic_tags=row['semantic_tags'].split(',') if row['semantic_tags'] else []
        )

    def get_hotspots(self, limit: int = 20, author: Optional[str] = None,
                     file_filter: Optional[str] = None,
                     since: Optional[str] = None,
                     until: Optional[str] = None) -> List[Dict]:
        """Return most-changed functions, optionally filtered by author or file path."""
        params: list = []
        where_clauses = []

        if author:
            where_clauses.append("author = ?")
            params.append(author)
        if file_filter:
            where_clauses.append("file_path LIKE ?")
            params.append(f"%{file_filter}%")
        if since:
            where_clauses.append("timestamp >= ?")
            params.append(parse_date_filter(since))
        if until:
            where_clauses.append("timestamp <= ?")
            params.append(parse_date_filter(until))

        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        rows = self.conn.execute(
            f"""
            SELECT
                function_name,
                file_path,
                COUNT(*) AS change_count,
                COUNT(DISTINCT author) AS author_count,
                SUM(CASE WHEN change_type = 'added'    THEN 1 ELSE 0 END) AS added,
                SUM(CASE WHEN change_type = 'modified' THEN 1 ELSE 0 END) AS modified,
                SUM(CASE WHEN change_type = 'deleted'  THEN 1 ELSE 0 END) AS deleted,
                MAX(timestamp) AS last_changed
            FROM function_changes
            {where}
            GROUP BY function_name, file_path
            ORDER BY change_count DESC
            LIMIT ?
            """,
            params + [limit]
        ).fetchall()

        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'change_count': r['change_count'],
                'author_count': r['author_count'],
                'added': r['added'],
                'modified': r['modified'],
                'deleted': r['deleted'],
                'last_changed': datetime.fromtimestamp(r['last_changed']) if r['last_changed'] else None,
            }
            for r in rows
        ]

    def get_file_blame_summary(self, file_path: str) -> List[Dict]:
        """Return all functions in a file with their primary owner and change count."""
        rows = self.conn.execute(
            """
            SELECT
                f.name,
                f.kind,
                f.start_line,
                f.end_line,
                f.language,
                b.author AS primary_owner,
                b.percentage AS ownership_pct,
                COALESCE(c.change_count, 0) AS change_count
            FROM functions f
            LEFT JOIN (
                SELECT function_name, file_path, author, percentage
                FROM blame_entries
                WHERE file_path = ?
                GROUP BY function_name, file_path
                HAVING percentage = MAX(percentage)
            ) b ON b.function_name = f.name AND b.file_path = f.file_path
            LEFT JOIN (
                SELECT function_name, file_path, COUNT(*) AS change_count
                FROM function_changes
                WHERE file_path = ?
                GROUP BY function_name, file_path
            ) c ON c.function_name = f.name AND c.file_path = f.file_path
            WHERE f.file_path = ?
            ORDER BY f.start_line
            """,
            (file_path, file_path, file_path)
        ).fetchall()

        return [
            {
                'name': r['name'],
                'kind': r['kind'],
                'start_line': r['start_line'],
                'end_line': r['end_line'],
                'language': r['language'],
                'primary_owner': r['primary_owner'] or '',
                'ownership_pct': r['ownership_pct'] or 0.0,
                'change_count': r['change_count'],
            }
            for r in rows
        ]

    def get_authors(self, limit: int = 20,
                    since: Optional[str] = None,
                    until: Optional[str] = None) -> List[Dict]:
        """Return per-author contribution stats across the whole repo."""
        where_clauses = ["author != ''"]
        params: list = []
        if since:
            where_clauses.append("timestamp >= ?")
            params.append(parse_date_filter(since))
        if until:
            where_clauses.append("timestamp <= ?")
            params.append(parse_date_filter(until))
        where_sql = "WHERE " + " AND ".join(where_clauses)
        rows = self.conn.execute(
            f"""
            SELECT
                author,
                COUNT(*)                        AS change_count,
                COUNT(DISTINCT function_name)   AS functions_touched,
                COUNT(DISTINCT file_path)       AS files_touched,
                SUM(lines_added)                AS total_added,
                SUM(lines_removed)              AS total_removed,
                MIN(timestamp)                  AS first_commit,
                MAX(timestamp)                  AS last_commit
            FROM function_changes
            {where_sql}
            GROUP BY author
            ORDER BY change_count DESC
            LIMIT ?
            """,
            params + [limit]
        ).fetchall()
        return [
            {
                'author': r['author'],
                'change_count': r['change_count'],
                'functions_touched': r['functions_touched'],
                'files_touched': r['files_touched'],
                'total_added': r['total_added'],
                'total_removed': r['total_removed'],
                'first_commit': datetime.fromtimestamp(r['first_commit']) if r['first_commit'] else None,
                'last_commit': datetime.fromtimestamp(r['last_commit']) if r['last_commit'] else None,
            }
            for r in rows
        ]

    def get_commits(self, limit: int = 20, file_filter: Optional[str] = None,
                    function_filter: Optional[str] = None,
                    author_filter: Optional[str] = None,
                    message_filter: Optional[str] = None,
                    since: Optional[str] = None,
                    until: Optional[str] = None) -> List[Dict]:
        """Return commits, optionally filtered by file, function, author, or message."""
        if file_filter or function_filter or author_filter or message_filter or since or until:
            params: list = []
            where = []
            need_join = bool(file_filter or function_filter)
            if file_filter:
                where.append("fc.file_path LIKE ?")
                params.append(f"%{file_filter}%")
            if function_filter:
                where.append("fc.function_name LIKE ?")
                params.append(f"%{function_filter}%")
            if author_filter:
                where.append("c.author LIKE ?")
                params.append(f"%{author_filter}%")
            if message_filter:
                if any(c in message_filter for c in r'|([\\'):
                    where.append("c.message REGEXP ?")
                    params.append(message_filter)
                else:
                    where.append("c.message LIKE ?")
                    params.append(f"%{message_filter}%")
            if since:
                where.append("c.timestamp >= ?")
                params.append(parse_date_filter(since))
            if until:
                where.append("c.timestamp <= ?")
                params.append(parse_date_filter(until))
            where_sql = "WHERE " + " AND ".join(where)
            join_sql = "JOIN function_changes fc ON fc.commit_hash = c.hash" if need_join else ""
            rows = self.conn.execute(
                f"""
                SELECT DISTINCT c.hash, c.author, c.timestamp, c.message, c.files_changed
                FROM commits c
                {join_sql}
                {where_sql}
                ORDER BY c.timestamp DESC
                LIMIT ?
                """,
                params + [limit]
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT hash, author, timestamp, message, files_changed FROM commits "
                "ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [
            {
                'hash': r['hash'],
                'author': r['author'],
                'timestamp': datetime.fromtimestamp(r['timestamp']) if r['timestamp'] else None,
                'message': r['message'],
                'files_changed': r['files_changed'],
            }
            for r in rows
        ]

    def get_function_info(self, file_path: str, function_name: str,
                          fuzzy_path: bool = False) -> Optional[FunctionInfo]:
        """Return FunctionInfo for a named function, optionally with fuzzy path match."""
        if fuzzy_path:
            row = self.conn.execute(
                "SELECT * FROM functions WHERE file_path LIKE ? AND name = ? LIMIT 1",
                (f"%{file_path}%", function_name)
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM functions WHERE file_path = ? AND name = ? LIMIT 1",
                (file_path, function_name)
            ).fetchone()
        if not row:
            return None
        return FunctionInfo(
            name=row['name'], file_path=row['file_path'], language=row['language'],
            start_line=row['start_line'], end_line=row['end_line'],
            kind=row['kind'], signature=row['signature']
        )

    def get_functions_by_pattern(self, pattern: str, kind: Optional[str] = None,
                                 file_filter: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """Find functions matching a name pattern (LIKE match)."""
        params: list = [f"%{pattern}%"]
        where = ["name LIKE ?"]
        if kind:
            where.append("kind = ?")
            params.append(kind)
        if file_filter:
            where.append("file_path LIKE ?")
            params.append(f"%{file_filter}%")
        params.append(limit)

        rows = self.conn.execute(
            f"SELECT * FROM functions WHERE {' AND '.join(where)} "
            "ORDER BY file_path, start_line LIMIT ?",
            params
        ).fetchall()
        return [
            {
                'name': r['name'], 'file_path': r['file_path'],
                'language': r['language'], 'start_line': r['start_line'],
                'end_line': r['end_line'], 'kind': r['kind'],
                'signature': r['signature'],
            }
            for r in rows
        ]

    def get_function_ages(self, file_filter: Optional[str] = None,
                          limit: int = 50, recent_first: bool = False) -> List[Dict]:
        """Return functions sorted by staleness (oldest-changed first by default)."""

        now = int(time.time())

        file_where = ""
        params: list = []
        if file_filter:
            file_where = "WHERE f.file_path LIKE ?"
            params.append(f"%{file_filter}%")

        order = "ASC" if not recent_first else "DESC"
        params.append(limit)

        rows = self.conn.execute(
            f"""
            SELECT
                f.name, f.file_path, f.kind,
                COALESCE(lc.last_ts, 0) AS last_changed,
                COALESCE(lc.change_count, 0) AS change_count,
                CASE WHEN lc.last_ts > 0 THEN ({now} - lc.last_ts) / 86400 ELSE -1 END AS days_ago
            FROM functions f
            LEFT JOIN (
                SELECT function_name, file_path,
                       MAX(timestamp) AS last_ts,
                       COUNT(*) AS change_count
                FROM function_changes
                GROUP BY function_name, file_path
            ) lc ON lc.function_name = f.name AND lc.file_path = f.file_path
            {file_where}
            ORDER BY
                CASE WHEN lc.last_ts IS NULL OR lc.last_ts = 0 THEN 1 ELSE 0 END DESC,
                lc.last_ts {order}
            LIMIT ?
            """,
            params
        ).fetchall()
        return [
            {
                'name': r['name'], 'file_path': r['file_path'],
                'kind': r['kind'],
                'last_changed': datetime.fromtimestamp(r['last_changed']) if r['last_changed'] else None,
                'days_ago': r['days_ago'],
                'change_count': r['change_count'],
            }
            for r in rows
        ]

    def get_timeline(self, months: int = 12) -> List[Dict]:
        """Return monthly activity aggregates for the last N months."""

        cutoff = int(time.time()) - months * 30 * 86400

        # Function changes per month
        change_rows = self.conn.execute(
            """
            SELECT
                strftime('%Y-%m', timestamp, 'unixepoch') AS month,
                COUNT(*) AS changes,
                COUNT(DISTINCT function_name) AS functions,
                COUNT(DISTINCT author) AS authors
            FROM function_changes
            WHERE timestamp > ?
            GROUP BY month
            ORDER BY month
            """,
            (cutoff,)
        ).fetchall()

        # Commits per month
        commit_rows = self.conn.execute(
            """
            SELECT
                strftime('%Y-%m', timestamp, 'unixepoch') AS month,
                COUNT(*) AS commits
            FROM commits
            WHERE timestamp > ?
            GROUP BY month
            ORDER BY month
            """,
            (cutoff,)
        ).fetchall()

        change_map = {r['month']: dict(r) for r in change_rows}
        commit_map = {r['month']: r['commits'] for r in commit_rows}
        all_months = sorted(set(list(change_map.keys()) + list(commit_map.keys())))

        results = []
        for m in all_months:
            cd = change_map.get(m, {})
            results.append({
                'month': m,
                'commits': commit_map.get(m, 0),
                'changes': cd.get('changes', 0),
                'functions': cd.get('functions', 0),
                'authors': cd.get('authors', 0),
            })
        return results

    def get_commit_diff(self, commit1: str, commit2: Optional[str] = None) -> List[Dict]:
        """Return function changes for a single commit or commit range."""
        if commit2:
            # Range: between two commits' timestamps
            rows = self.conn.execute(
                """
                SELECT fc.*, c.timestamp AS commit_ts
                FROM function_changes fc
                JOIN commits c ON c.hash = fc.commit_hash
                WHERE c.timestamp BETWEEN
                    (SELECT MIN(timestamp) FROM commits WHERE hash LIKE ?)
                    AND
                    (SELECT MAX(timestamp) FROM commits WHERE hash LIKE ?)
                ORDER BY fc.timestamp, fc.file_path
                """,
                (f"{commit1}%", f"{commit2}%")
            ).fetchall()
        else:
            # Single commit (exact or prefix match)
            rows = self.conn.execute(
                "SELECT * FROM function_changes WHERE commit_hash = ? OR commit_hash LIKE ? "
                "ORDER BY file_path, function_name",
                (commit1, f"{commit1}%")
            ).fetchall()

        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'change_type': r['change_type'],
                'lines_added': r['lines_added'],
                'lines_removed': r['lines_removed'],
                'author': r['author'],
                'commit_hash': r['commit_hash'],
            }
            for r in rows
        ]

    def get_release_diff_summary(self, since_hash: Optional[str],
                                   until_hash: str) -> Dict:
        """Summarise function changes between two commits (for release diffs).

        Uses commit rowid ordering to find the exact set of commits in the range,
        avoiding timestamp-boundary issues when commits share the same second.

        Args:
            since_hash: Previous tag's commit hash (None for first tag = include all up to until_hash)
            until_hash: Current tag's commit hash
        """
        if since_hash:
            # Get function changes from commits between the two tag commits.
            # Use (timestamp, hash) ordering to handle same-second commits correctly.
            query_where = """
                WHERE commit_hash IN (
                    SELECT hash FROM commits
                    WHERE (timestamp, hash) > (
                        SELECT timestamp, hash FROM commits WHERE hash = ?
                    )
                    AND (timestamp, hash) <= (
                        SELECT timestamp, hash FROM commits WHERE hash = ?
                    )
                )
            """
            params = (since_hash, until_hash)
        else:
            # First tag: all commits up to and including until_hash
            query_where = """
                WHERE commit_hash IN (
                    SELECT hash FROM commits
                    WHERE (timestamp, hash) <= (
                        SELECT timestamp, hash FROM commits WHERE hash = ?
                    )
                )
            """
            params = (until_hash,)

        rows = self.conn.execute(
            f"SELECT change_type, COUNT(*) AS cnt FROM function_changes {query_where} GROUP BY change_type",
            params
        ).fetchall()
        summary = {r['change_type']: r['cnt'] for r in rows}

        files_row = self.conn.execute(
            f"SELECT COUNT(DISTINCT file_path) AS cnt FROM function_changes {query_where}",
            params
        ).fetchone()

        authors_row = self.conn.execute(
            f"SELECT COUNT(DISTINCT author) AS cnt FROM function_changes {query_where}",
            params
        ).fetchone()

        return {
            'added': summary.get('added', 0),
            'modified': summary.get('modified', 0),
            'deleted': summary.get('deleted', 0),
            'total': sum(summary.values()),
            'files_touched': files_row['cnt'] if files_row else 0,
            'authors': authors_row['cnt'] if authors_row else 0,
        }

    def get_file_report(self, file_path: str) -> Optional[Dict]:
        """Comprehensive file report: stats + per-function detail with age."""

        now = int(time.time())

        # Try exact match first, then fuzzy
        funcs = self.conn.execute(
            "SELECT * FROM functions WHERE file_path = ? ORDER BY start_line",
            (file_path,)
        ).fetchall()
        if not funcs:
            funcs = self.conn.execute(
                "SELECT * FROM functions WHERE file_path LIKE ? ORDER BY start_line",
                (f"%{file_path}%",)
            ).fetchall()
        if not funcs:
            return None

        # Fuzzy match may span multiple files — restrict to the first match
        actual_path = funcs[0]['file_path']
        funcs = [f for f in funcs if f['file_path'] == actual_path]

        # Blame data
        blame_rows = self.conn.execute(
            """
            SELECT function_name, author, percentage
            FROM blame_entries WHERE file_path = ?
            ORDER BY percentage DESC
            """,
            (actual_path,)
        ).fetchall()
        blame_map: Dict[str, Tuple] = {}
        for r in blame_rows:
            if r['function_name'] not in blame_map:
                blame_map[r['function_name']] = (r['author'], r['percentage'])

        # Change counts + last_changed
        change_rows = self.conn.execute(
            """
            SELECT function_name, COUNT(*) AS change_count, MAX(timestamp) AS last_ts
            FROM function_changes WHERE file_path = ?
            GROUP BY function_name
            """,
            (actual_path,)
        ).fetchall()
        change_map = {r['function_name']: (r['change_count'], r['last_ts']) for r in change_rows}

        total_changes = sum(v[0] for v in change_map.values())
        unique_owners = len(set(v[0] for v in blame_map.values())) if blame_map else 0
        language = funcs[0]['language'] if funcs else ''

        func_list = []
        for f in funcs:
            owner, pct = blame_map.get(f['name'], ('', 0.0))
            changes, last_ts = change_map.get(f['name'], (0, None))
            days = int((now - last_ts) / 86400) if last_ts else -1
            func_list.append({
                'name': f['name'], 'kind': f['kind'],
                'start_line': f['start_line'], 'end_line': f['end_line'],
                'owner': owner, 'ownership_pct': pct,
                'change_count': changes,
                'last_changed': datetime.fromtimestamp(last_ts) if last_ts else None,
                'days_ago': days,
            })

        return {
            'file_path': actual_path,
            'language': language,
            'total_functions': len(funcs),
            'total_changes': total_changes,
            'unique_owners': unique_owners,
            'functions': func_list,
        }

    def get_export_data(self, sections: Optional[List[str]] = None) -> Dict:
        """Gather data for export. Sections: functions, changes, blame, authors, timeline, hotspots."""
        all_sections = ['functions', 'changes', 'blame', 'authors', 'timeline', 'hotspots']
        if not sections:
            sections = all_sections

        data: Dict = {}
        if 'functions' in sections:
            rows = self.conn.execute(
                "SELECT name, file_path, language, kind, start_line, end_line, signature "
                "FROM functions ORDER BY file_path, start_line"
            ).fetchall()
            data['functions'] = [dict(r) for r in rows]

        if 'changes' in sections:
            rows = self.conn.execute(
                "SELECT function_name, file_path, commit_hash, change_type, "
                "lines_added, lines_removed, author, timestamp, message "
                "FROM function_changes ORDER BY timestamp"
            ).fetchall()
            data['changes'] = [dict(r) for r in rows]

        if 'blame' in sections:
            rows = self.conn.execute(
                "SELECT function_name, file_path, author, line_count, percentage, commit_hash "
                "FROM blame_entries ORDER BY file_path, function_name"
            ).fetchall()
            data['blame'] = [dict(r) for r in rows]

        if 'authors' in sections:
            data['authors'] = self.get_authors()
        if 'timeline' in sections:
            data['timeline'] = self.get_timeline()
        if 'hotspots' in sections:
            data['hotspots'] = self.get_hotspots(limit=9999)

        return data

    def get_stability_scores(self, limit: int = 30,
                             file_filter: Optional[str] = None) -> List[Dict]:
        """Return functions ranked by stability score (0.0=volatile, 1.0=stable).

        Includes functions with zero changes (scored 1.0 = perfectly stable).
        """
        now = int(time.time())

        # Get change data from hotspots
        hotspots = self.get_hotspots(limit=9999, file_filter=file_filter)
        hotspot_map = {(h['function_name'], h['file_path']): h for h in hotspots}

        max_changes = max((h['change_count'] for h in hotspots), default=0)
        max_authors = max((h['author_count'] for h in hotspots), default=0)

        # Get ALL functions (including those with zero changes)
        where = ""
        params: list = []
        if file_filter:
            where = "WHERE file_path LIKE ?"
            params.append(f"%{file_filter}%")
        all_funcs = self.conn.execute(
            f"SELECT name, file_path FROM functions {where} "
            "ORDER BY file_path, name",
            params
        ).fetchall()

        scored = []
        seen = set()
        for f in all_funcs:
            key = (f['name'], f['file_path'])
            if key in seen:
                continue
            seen.add(key)

            h = hotspot_map.get(key)
            if h:
                change_rate = h['change_count'] / max_changes if max_changes else 0
                last_ts = h['last_changed'].timestamp() if h['last_changed'] else 0
                days_ago = (now - last_ts) / 86400 if last_ts else 365
                recency = min(days_ago / 365, 1.0)
                author_spread = h['author_count'] / max_authors if max_authors else 0
                change_count = h['change_count']
                author_count = h['author_count']
            else:
                # Zero-change function — maximum stability
                change_rate = 0
                days_ago = 365
                recency = 1.0
                author_spread = 0
                change_count = 0
                author_count = 0

            score = round((1 - change_rate) * 0.5 + recency * 0.3 + (1 - author_spread) * 0.2, 3)

            if score >= 0.8:
                rating = 'stable'
            elif score >= 0.5:
                rating = 'moderate'
            elif score >= 0.3:
                rating = 'volatile'
            else:
                rating = 'critical'

            scored.append({
                'function_name': f['name'],
                'file_path': f['file_path'],
                'stability_score': score,
                'change_count': change_count,
                'author_count': author_count,
                'days_ago': int(days_ago),
                'rating': rating,
            })

        scored.sort(key=lambda x: x['stability_score'], reverse=True)
        return scored[:limit]

    def get_commits_by_month(self) -> List[Dict]:
        """Return all commits grouped by month with messages."""
        rows = self.conn.execute(
            """
            SELECT
                strftime('%Y-%m', timestamp, 'unixepoch') AS month,
                message,
                author,
                hash
            FROM commits
            ORDER BY timestamp
            """
        ).fetchall()
        result: Dict[str, list] = {}
        for r in rows:
            month = r['month']
            if month not in result:
                result[month] = []
            result[month].append({
                'message': r['message'] or '',
                'author': r['author'],
                'hash': r['hash'],
            })
        return [{'month': m, 'commits': cs} for m, cs in sorted(result.items())]

    def get_fragile_functions(self, limit: int = 15,
                              since: Optional[str] = None,
                              until: Optional[str] = None) -> List[Dict]:
        """Functions with 5+ changes, mostly modifications — fragile zones."""
        where_clauses: list = []
        params: list = []
        if since:
            where_clauses.append("timestamp >= ?")
            params.append(parse_date_filter(since))
        if until:
            where_clauses.append("timestamp <= ?")
            params.append(parse_date_filter(until))
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        rows = self.conn.execute(
            f"""
            SELECT
                function_name, file_path,
                COUNT(*) AS change_count,
                COUNT(DISTINCT author) AS author_count,
                SUM(CASE WHEN change_type = 'modified' THEN 1 ELSE 0 END) AS modify_count,
                MIN(timestamp) AS first_change,
                MAX(timestamp) AS last_change
            FROM function_changes
            {where_sql}
            GROUP BY function_name, file_path
            HAVING COUNT(*) >= 5 AND SUM(CASE WHEN change_type = 'modified' THEN 1 ELSE 0 END) >= 3
            ORDER BY change_count DESC
            LIMIT ?
            """,
            params + [limit]
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'change_count': r['change_count'],
                'author_count': r['author_count'],
                'modify_count': r['modify_count'],
                'first_change': datetime.fromtimestamp(r['first_change']) if r['first_change'] else None,
                'last_change': datetime.fromtimestamp(r['last_change']) if r['last_change'] else None,
            }
            for r in rows
        ]

    def get_stale_functions(self, limit: int = 15,
                            since: Optional[str] = None,
                            until: Optional[str] = None) -> List[Dict]:
        """Functions sorted by oldest last-change — stale/forgotten code."""
        extra_where: list = []
        params: list = []
        if since:
            extra_where.append("fc.timestamp >= ?")
            params.append(parse_date_filter(since))
        if until:
            extra_where.append("fc.timestamp <= ?")
            params.append(parse_date_filter(until))
        extra_sql = ("AND " + " AND ".join(extra_where)) if extra_where else ""
        rows = self.conn.execute(
            f"""
            SELECT
                f.name AS function_name, f.file_path, f.kind, f.language,
                MAX(fc.timestamp) AS last_changed,
                COUNT(fc.id) AS total_changes
            FROM functions f
            LEFT JOIN function_changes fc ON f.name = fc.function_name AND f.file_path = fc.file_path {extra_sql}
            GROUP BY f.name, f.file_path
            ORDER BY last_changed ASC NULLS FIRST
            LIMIT ?
            """,
            params + [limit]
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'kind': r['kind'],
                'language': r['language'],
                'last_changed': datetime.fromtimestamp(r['last_changed']) if r['last_changed'] else None,
                'total_changes': r['total_changes'],
            }
            for r in rows
        ]

    def get_coauthorship_patterns(self, limit: int = 20) -> List[Dict]:
        """Functions touched by 2+ authors — convergence points."""
        rows = self.conn.execute(
            """
            SELECT
                function_name, file_path,
                COUNT(DISTINCT author) AS author_count,
                GROUP_CONCAT(DISTINCT author) AS authors,
                COUNT(*) AS change_count
            FROM function_changes
            GROUP BY function_name, file_path
            HAVING COUNT(DISTINCT author) >= 2
            ORDER BY author_count DESC, change_count DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'author_count': r['author_count'],
                'authors': r['authors'].split(',') if r['authors'] else [],
                'change_count': r['change_count'],
            }
            for r in rows
        ]

    def get_bus_factor_by_directory(self) -> List[Dict]:
        """Return directories where one author owns >80% of blame lines.

        High bus-factor risk means knowledge concentration in one person.
        """
        rows = self.conn.execute(
            """
            SELECT
                CASE
                    WHEN INSTR(file_path, '/') > 0
                    THEN SUBSTR(file_path, 1, INSTR(file_path, '/') - 1)
                    ELSE '.'
                END AS directory,
                author,
                SUM(line_count) AS lines
            FROM blame_entries
            GROUP BY directory, author
            """
        ).fetchall()

        # Aggregate per directory
        dir_totals: Dict[str, int] = {}
        dir_authors: Dict[str, Dict[str, int]] = {}
        for r in rows:
            d = r['directory']
            a = r['author']
            lines = r['lines']
            dir_totals[d] = dir_totals.get(d, 0) + lines
            if d not in dir_authors:
                dir_authors[d] = {}
            dir_authors[d][a] = dir_authors[d].get(a, 0) + lines

        results = []
        for d, total in dir_totals.items():
            if total == 0:
                continue
            for author, lines in dir_authors[d].items():
                pct = lines / total * 100
                if pct > 80:
                    results.append({
                        'directory': d,
                        'dominant_author': author,
                        'percentage': round(pct, 1),
                        'total_lines': total,
                        'author_count': len(dir_authors[d]),
                    })
        results.sort(key=lambda x: x['percentage'], reverse=True)
        return results

    def get_feature_expansion(self) -> List[Dict]:
        """Functions added per month — tracks when new capabilities appeared."""
        rows = self.conn.execute(
            """
            SELECT
                strftime('%Y-%m', timestamp, 'unixepoch') AS month,
                file_path,
                function_name,
                author
            FROM function_changes
            WHERE change_type = 'added'
            ORDER BY timestamp
            """
        ).fetchall()
        # Group by month
        months: Dict[str, Dict] = {}
        seen_files: Dict[str, set] = {}  # track which files had first additions
        cumulative = 0
        for r in rows:
            month = r['month']
            if month not in months:
                months[month] = {'new_functions': 0, 'files': set(), 'dirs': set(), 'authors': set()}
                seen_files[month] = set()
            months[month]['new_functions'] += 1
            months[month]['files'].add(r['file_path'])
            months[month]['authors'].add(r['author'])
            # Extract directory
            parts = r['file_path'].rsplit('/', 1)
            if len(parts) > 1:
                months[month]['dirs'].add(parts[0] + '/')

        result = []
        for m in sorted(months.keys()):
            d = months[m]
            cumulative += d['new_functions']
            result.append({
                'month': m,
                'new_functions': d['new_functions'],
                'new_files': sorted(d['files'])[:10],
                'expanding_areas': sorted(d['dirs'])[:10],
                'cumulative_functions': cumulative,
            })
        return result

    def get_coupled_functions(self, function_name: str, file_path: str = None, limit: int = 20) -> List[Dict]:
        """Find functions that change in the same commits as the target."""
        # Get commit hashes where target function changed
        if file_path:
            commit_rows = self.conn.execute(
                "SELECT DISTINCT commit_hash FROM function_changes WHERE function_name = ? AND file_path LIKE ?",
                (function_name, f"%{file_path}%")
            ).fetchall()
        else:
            commit_rows = self.conn.execute(
                "SELECT DISTINCT commit_hash FROM function_changes WHERE function_name = ?",
                (function_name,)
            ).fetchall()
        if not commit_rows:
            return []
        hashes = [r['commit_hash'] for r in commit_rows]
        placeholders = ','.join('?' * len(hashes))
        rows = self.conn.execute(
            f"""SELECT function_name, file_path, COUNT(*) as co_change_count
                FROM function_changes
                WHERE commit_hash IN ({placeholders})
                  AND NOT (function_name = ?)
                GROUP BY function_name, file_path
                ORDER BY co_change_count DESC
                LIMIT ?""",
            hashes + [function_name, limit]
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'co_change_count': r['co_change_count'],
                'total_target_commits': len(hashes),
                'coupling_ratio': round(r['co_change_count'] / len(hashes), 2),
            }
            for r in rows
        ]

    def get_changed_functions_since(self, since_ts: int, limit: int = 50) -> List[Dict]:
        """Return functions modified since a timestamp."""
        rows = self.conn.execute(
            """SELECT function_name, file_path, MAX(timestamp) as last_changed,
                      COUNT(*) as change_count
               FROM function_changes
               WHERE timestamp >= ?
               GROUP BY function_name, file_path
               ORDER BY MAX(timestamp) DESC
               LIMIT ?""",
            (since_ts, limit)
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'last_changed': datetime.fromtimestamp(r['last_changed']) if r['last_changed'] else None,
                'change_count': r['change_count'],
            }
            for r in rows
        ]

    def get_file_lifecycle(self, file_path: str) -> Optional[Dict]:
        """Aggregate lifecycle data for a file."""
        rows = self.conn.execute(
            """SELECT MIN(timestamp) as first_commit, MAX(timestamp) as last_commit,
                      COUNT(*) as total_changes,
                      COUNT(DISTINCT author) as unique_authors,
                      SUM(CASE WHEN change_type = 'added' THEN 1 ELSE 0 END) as added,
                      SUM(CASE WHEN change_type = 'modified' THEN 1 ELSE 0 END) as modified,
                      SUM(CASE WHEN change_type = 'deleted' THEN 1 ELSE 0 END) as deleted
               FROM function_changes
               WHERE file_path LIKE ?""",
            (f"%{file_path}%",)
        ).fetchone()
        if not rows or rows['total_changes'] == 0:
            return None
        # Get unique functions
        func_rows = self.conn.execute(
            "SELECT DISTINCT function_name FROM function_changes WHERE file_path LIKE ?",
            (f"%{file_path}%",)
        ).fetchall()
        return {
            'file_path': file_path,
            'first_commit': datetime.fromtimestamp(rows['first_commit']) if rows['first_commit'] else None,
            'last_commit': datetime.fromtimestamp(rows['last_commit']) if rows['last_commit'] else None,
            'total_changes': rows['total_changes'],
            'unique_authors': rows['unique_authors'],
            'functions_added': rows['added'],
            'functions_modified': rows['modified'],
            'functions_deleted': rows['deleted'],
            'unique_functions': len(func_rows),
        }

    def get_churn_by_directory(self, months: int = 12, dir_filter: Optional[str] = None) -> List[Dict]:
        """Group function changes by directory and month."""
        since_ts = int(time.time()) - months * 30 * 86400
        where_extra = ""
        params: list = [since_ts]
        if dir_filter:
            where_extra = "AND file_path LIKE ?"
            params.append(f"%{dir_filter}%")
        rows = self.conn.execute(
            f"""SELECT
                    CASE WHEN INSTR(file_path, '/') > 0
                         THEN SUBSTR(file_path, 1, INSTR(file_path, '/') - 1)
                         ELSE '.'
                    END as directory,
                    strftime('%Y-%m', timestamp, 'unixepoch') as month,
                    COUNT(*) as changes
                FROM function_changes
                WHERE timestamp >= ? {where_extra}
                GROUP BY directory, month
                ORDER BY directory, month""",
            params
        ).fetchall()
        return [dict(r) for r in rows]

    def get_untested_changes(self, limit: int = 20, file_filter: Optional[str] = None) -> List[Dict]:
        """Find function changes where no test file changed in the same commit."""
        where_extra = ""
        params: list = []
        if file_filter:
            where_extra = "AND fc.file_path LIKE ?"
            params.append(f"%{file_filter}%")
        rows = self.conn.execute(
            f"""SELECT fc.function_name, fc.file_path, fc.commit_hash,
                       fc.change_type, fc.author, fc.timestamp, fc.message
                FROM function_changes fc
                WHERE NOT EXISTS (
                    SELECT 1 FROM function_changes fc2
                    WHERE fc2.commit_hash = fc.commit_hash
                      AND (fc2.file_path LIKE '%test_%'
                           OR fc2.file_path LIKE '%_test.%'
                           OR fc2.file_path LIKE '%.test.%'
                           OR fc2.file_path LIKE '%__tests__%')
                )
                AND fc.file_path NOT LIKE '%test_%'
                AND fc.file_path NOT LIKE '%_test.%'
                AND fc.file_path NOT LIKE '%.test.%'
                AND fc.file_path NOT LIKE '%__tests__%'
                {where_extra}
                ORDER BY fc.timestamp DESC
                LIMIT ?""",
            params + [limit]
        ).fetchall()
        return [
            {
                'function_name': r['function_name'],
                'file_path': r['file_path'],
                'commit_hash': r['commit_hash'],
                'change_type': r['change_type'],
                'author': r['author'],
                'timestamp': datetime.fromtimestamp(r['timestamp']) if r['timestamp'] else None,
                'message': r['message'],
            }
            for r in rows
        ]

    def get_stats(self) -> Dict:
        commits = self.conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        functions = self.conn.execute("SELECT COUNT(*) FROM functions").fetchone()[0]
        changes = self.conn.execute("SELECT COUNT(*) FROM function_changes").fetchone()[0]
        blame = self.conn.execute("SELECT COUNT(*) FROM blame_entries").fetchone()[0]
        return {"commits": commits, "functions": functions, "changes": changes, "blame_entries": blame}

    # -- FTS5 search methods --

    @staticmethod
    def _split_identifiers(text: str) -> str:
        """Split CamelCase, snake_case, and number-boundary identifiers for better FTS matching.
        'GitMiningEngine' -> 'GitMiningEngine Git Mining Engine'
        'get_blame_for_file' -> 'get_blame_for_file get blame for file'
        'test123func' -> 'test123func test 123 func'
        """
        import re
        words = set()
        # Find identifier-like tokens
        for token in re.findall(r'[A-Za-z_]\w*', text):
            words.add(token)
            # Split CamelCase
            parts = re.sub(r'([a-z])([A-Z])', r'\1 \2', token)
            parts = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', parts)
            # Split number boundaries (letters<->digits)
            parts = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', parts)
            parts = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', parts)
            if ' ' in parts:
                for p in parts.split():
                    words.add(p.lower())
            # Split snake_case
            if '_' in token:
                for p in token.split('_'):
                    if p:
                        words.add(p.lower())
        extra = words - {text}
        if not extra:
            return text
        return text + ' ' + ' '.join(extra)

    def rebuild_search_index(self) -> int:
        """Rebuild the FTS5 search index from all tables. Returns doc count."""
        self.conn.execute("DELETE FROM search_index")
        count = 0

        # Index commits (use cursor iteration, not fetchall)
        for row in self.conn.execute("SELECT * FROM commits"):
            ts = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d')
            content = self._split_identifiers(row['message'])
            self.conn.execute(
                "INSERT INTO search_index (type, name, file_path, author, content, detail) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ('commit', row['hash'][:8], '', row['author'],
                 content,
                 f"commit {row['hash'][:8]} on {ts} ({row['files_changed']} files)")
            )
            count += 1

        # Batch-load primary author per function from blame (eliminates N+1 queries)
        blame_authors: Dict[tuple, str] = {}
        for row in self.conn.execute(
            "SELECT function_name, file_path, author FROM blame_entries ORDER BY percentage DESC"
        ):
            key = (row['function_name'], row['file_path'])
            if key not in blame_authors:
                blame_authors[key] = row['author']

        # Index functions
        for row in self.conn.execute("SELECT * FROM functions"):
            author = blame_authors.get((row['name'], row['file_path']), '')
            name_expanded = self._split_identifiers(row['name'])
            sig_expanded = self._split_identifiers(row['signature'])
            self.conn.execute(
                "INSERT INTO search_index (type, name, file_path, author, content, detail) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ('function', row['name'], row['file_path'], author,
                 f"{row['kind']} {name_expanded} {sig_expanded}",
                 f"{row['kind']} in {row['file_path']} lines {row['start_line']}-{row['end_line']} ({row['language']})")
            )
            count += 1

        # Index function changes
        for row in self.conn.execute("SELECT * FROM function_changes"):
            name_expanded = self._split_identifiers(row['function_name'])
            msg_expanded = self._split_identifiers(row['message'])
            self.conn.execute(
                "INSERT INTO search_index (type, name, file_path, author, content, detail) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ('change', row['function_name'], row['file_path'], row['author'],
                 f"{row['change_type']} {name_expanded} {msg_expanded}",
                 f"{row['change_type']} +{row['lines_added']}/-{row['lines_removed']} in {row['commit_hash'][:8]}")
            )
            count += 1

        self.conn.commit()
        return count

    @staticmethod
    def _sanitize_fts_query(query: str) -> Optional[str]:
        """Sanitize user input for safe FTS5 querying.
        Tokenizes into words and wraps each in double quotes for literal matching."""
        import re
        # Extract alphanumeric words only, stripping all FTS5 special syntax
        tokens = re.findall(r"[A-Za-z0-9_]+", query)
        if not tokens:
            return None
        # Each token wrapped in quotes = literal match, space-separated = implicit AND
        return ' '.join(f'"{t}"' for t in tokens)

    def search(self, query: str, limit: int = 20,
               type_filter: Optional[str] = None) -> List[Dict]:
        """Full-text search across all indexed data."""
        if not query or not query.strip():
            return []

        fts_query = self._sanitize_fts_query(query)
        if not fts_query:
            return []

        try:
            if type_filter:
                rows = self.conn.execute(
                    "SELECT type, name, file_path, author, content, detail, "
                    "rank FROM search_index WHERE search_index MATCH ? "
                    "AND type = ? ORDER BY rank LIMIT ?",
                    (fts_query, type_filter, limit)
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT type, name, file_path, author, content, detail, "
                    "rank FROM search_index WHERE search_index MATCH ? "
                    "ORDER BY rank LIMIT ?",
                    (fts_query, limit)
                ).fetchall()
            return [
                {
                    'type': r['type'],
                    'name': r['name'],
                    'file_path': r['file_path'],
                    'author': r['author'],
                    'content': r['content'],
                    'detail': r['detail'],
                    'score': -r['rank'],
                }
                for r in rows
            ]
        except sqlite3.OperationalError:
            return []

    def get_language_stats(self) -> List[Dict]:
        """Return function count per language, ordered by count descending."""
        rows = self.conn.execute(
            "SELECT language, COUNT(*) as count FROM functions GROUP BY language ORDER BY count DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def _get_stability_for_report(self) -> list:
        """Return bounded stability data for reports: all volatile/critical + sample of others."""
        all_scores = self.get_stability_scores(limit=999999)
        volatile = [s for s in all_scores if s['rating'] in ('volatile', 'critical')]
        moderate = [s for s in all_scores if s['rating'] == 'moderate' and s['change_count'] > 0][:200]
        stable = [s for s in all_scores if s['rating'] == 'stable' and s['change_count'] > 0][:100]
        result = volatile + moderate + stable
        result.sort(key=lambda x: x['stability_score'])
        return result

    def get_report_data(self) -> Dict:
        """Gather all data needed for the HTML report in one call."""
        return {
            'stats': self.get_stats(),
            'timeline': self.get_timeline(months=120),
            'hotspots': self.get_hotspots(limit=20),
            'stability': self._get_stability_for_report(),
            'authors': self.get_authors(limit=100),
            'languages': self.get_language_stats(),
        }

    # -- Embedding storage and search methods --

    def save_embedding(self, type_: str, ref_id: str, text: str,
                       vector: bytes, model: str) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO embeddings (type, ref_id, text, vector, model) "
            "VALUES (?, ?, ?, ?, ?)",
            (type_, ref_id, text, vector, model)
        )

    def save_embeddings_batch(self, entries: list) -> None:
        """Save a batch of (type, ref_id, text, vector_bytes, model) tuples."""
        self.conn.executemany(
            "INSERT OR REPLACE INTO embeddings (type, ref_id, text, vector, model) "
            "VALUES (?, ?, ?, ?, ?)",
            entries
        )
        self.conn.commit()

    def delete_embeddings_for_file(self, file_path: str) -> None:
        self.conn.execute(
            "DELETE FROM embeddings WHERE type = 'function' AND ref_id LIKE ?",
            (f'%::{file_path}',)
        )

    def clear_embeddings(self) -> None:
        self.conn.execute("DELETE FROM embeddings")
        self.conn.commit()

    def get_embedded_ref_ids(self, type_: str) -> set:
        rows = self.conn.execute(
            "SELECT ref_id FROM embeddings WHERE type = ?", (type_,)
        ).fetchall()
        return {r['ref_id'] for r in rows}

    def has_embeddings(self) -> bool:
        try:
            row = self.conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()
            return row[0] > 0
        except sqlite3.OperationalError:
            return False

    def get_embedding_stats(self) -> Dict:
        try:
            total = self.conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
            if total == 0:
                return {}
            funcs = self.conn.execute(
                "SELECT COUNT(*) FROM embeddings WHERE type = 'function'"
            ).fetchone()[0]
            commits = self.conn.execute(
                "SELECT COUNT(*) FROM embeddings WHERE type = 'commit'"
            ).fetchone()[0]
            model = self.get_meta('embed_model') or 'unknown'
            dim = self.get_meta('embed_dim') or '?'
            return {
                'total': total, 'functions': funcs, 'commits': commits,
                'model': model, 'dim': dim,
            }
        except sqlite3.OperationalError:
            return {}

    def get_all_embeddings(self, type_filter: str = None) -> Tuple[list, 'np.ndarray']:
        """Load all embeddings as metadata list + numpy matrix.
        Returns (metadata_list, vectors_matrix). Matrix rows are unit vectors."""
        import numpy as np

        query = "SELECT type, ref_id, vector FROM embeddings"
        params = ()
        if type_filter:
            query += " WHERE type = ?"
            params = (type_filter,)

        rows = self.conn.execute(query, params).fetchall()
        if not rows:
            return [], np.empty((0, 0), dtype=np.float32)

        metadata = [{'type': r['type'], 'ref_id': r['ref_id']} for r in rows]
        dim = len(rows[0]['vector']) // 4  # float32 = 4 bytes
        matrix = np.zeros((len(rows), dim), dtype=np.float32)
        for i, r in enumerate(rows):
            matrix[i] = np.frombuffer(r['vector'], dtype=np.float32)

        return metadata, matrix

    def semantic_search(self, query_vector: 'np.ndarray', limit: int = 100,
                        type_filter: str = None, enrich: bool = True) -> list:
        """Cosine similarity search. Returns ranked results with scores.

        If enrich=True, looks up full metadata from source tables.
        """
        import numpy as np

        metadata, matrix = self.get_all_embeddings(type_filter)
        if len(metadata) == 0:
            return []

        # query_vector should already be L2-normalized
        scores = matrix @ query_vector  # dot product = cosine for unit vectors
        top_indices = np.argsort(scores)[::-1][:limit]

        results = []
        for idx in top_indices:
            score = float(scores[idx])
            if score < 0.05:  # minimum cosine threshold — below this is noise
                break
            meta = metadata[idx]
            if enrich:
                ref_id = meta['ref_id']
                type_ = meta['type']
                if type_ == 'function' and '::' in ref_id:
                    name, fpath = ref_id.split('::', 1)
                    key = ('function', name, fpath)
                elif type_ == 'commit':
                    key = ('commit', ref_id[:8], '')
                else:
                    key = (type_, ref_id, '')
                data = self._enrich_semantic_result(meta, key)
                data['score'] = score
                data['cosine'] = score
                data['source'] = 'semantic'
                # Add relevance label for standalone semantic search
                if score >= 0.40:
                    data['relevance'] = 'HIGH'
                elif score >= 0.30:
                    data['relevance'] = 'GOOD'
                elif score >= 0.20:
                    data['relevance'] = 'FAIR'
                elif score >= 0.10:
                    data['relevance'] = 'LOW'
                else:
                    data['relevance'] = 'WEAK'
                results.append(data)
            else:
                results.append({
                    'type': meta['type'],
                    'ref_id': meta['ref_id'],
                    'score': score,
                    'cosine': score,
                    'source': 'semantic',
                })
        return results

    def _enrich_semantic_result(self, sem_data: dict, key: tuple) -> dict:
        """Look up full metadata for a semantic-only result from the source tables."""
        type_ = sem_data['type']
        ref_id = sem_data['ref_id']

        if type_ == 'function' and '::' in ref_id:
            name, fpath = ref_id.split('::', 1)
            row = self.conn.execute(
                "SELECT kind, start_line, end_line, language FROM functions "
                "WHERE name = ? AND file_path = ? LIMIT 1", (name, fpath)
            ).fetchone()
            if row:
                return {
                    'type': 'function', 'name': name, 'file_path': fpath,
                    'author': '', 'content': '',
                    'detail': f"{row['kind']} in {fpath} lines {row['start_line']}-{row['end_line']} ({row['language']})",
                }
        elif type_ == 'commit':
            row = self.conn.execute(
                "SELECT hash, author, timestamp, message, files_changed FROM commits "
                "WHERE hash = ? LIMIT 1", (ref_id,)
            ).fetchone()
            if row:
                ts = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%d')
                return {
                    'type': 'commit', 'name': row['hash'][:8], 'file_path': '',
                    'author': row['author'], 'content': row['message'][:100],
                    'detail': f"commit {row['hash'][:8]} on {ts} ({row['files_changed']} files)",
                }

        # Fallback: minimal data from key
        return {
            'type': type_, 'name': key[1], 'file_path': key[2],
            'author': '', 'content': '', 'detail': '',
        }

    def hybrid_search(self, query: str, query_vector: 'np.ndarray | None',
                      limit: int = 20, type_filter: str = None,
                      fts5_weight: float = 0.5, semantic_weight: float = 0.5) -> list:
        """Full hybrid pipeline: FTS5 + semantic, fused via RRF + cosine blending.

        Pure RRF throws away score magnitude (a cosine of 0.57 and 0.22 get
        nearly identical RRF scores if they're rank 1 vs rank 5). This pipeline
        uses RRF for cross-method agreement detection, then blends in the raw
        cosine similarity to preserve the embedding model's discriminative signal.

        Final score = RRF_component + cosine_component, where:
          RRF_component = weighted RRF (rank-based, cross-method agreement)
          cosine_component = normalized cosine similarity (score-based, discrimination)
        """
        k = 60  # RRF constant
        overfetch = limit * 3

        # 1. FTS5 results
        fts5_results = self.search(query, limit=overfetch, type_filter=type_filter)
        fts5_ranked = {}
        for rank, r in enumerate(fts5_results):
            key = (r['type'], r.get('name', ''), r.get('file_path', ''))
            fts5_ranked[key] = {'rank': rank, 'data': r}

        # 2. Semantic results (with raw cosine scores)
        sem_ranked = {}
        if query_vector is not None and self.has_embeddings():
            sem_results = self.semantic_search(
                query_vector, limit=overfetch, type_filter=type_filter, enrich=False
            )
            for rank, r in enumerate(sem_results):
                ref_id = r['ref_id']
                if r['type'] == 'function' and '::' in ref_id:
                    name, fpath = ref_id.split('::', 1)
                    key = ('function', name, fpath)
                elif r['type'] == 'commit':
                    key = ('commit', ref_id[:8], '')
                else:
                    key = (r['type'], ref_id, '')
                sem_ranked[key] = {'rank': rank, 'data': r, 'cosine': r.get('cosine', 0.0)}

        # 3. Compute max cosine for normalization
        max_cosine = max((v['cosine'] for v in sem_ranked.values()), default=0.0)
        if max_cosine < 0.05:
            max_cosine = 1.0  # avoid division by near-zero

        # 4. Fused scoring: RRF + cosine blending
        # RRF handles rank-based agreement; cosine preserves score discrimination
        # The blend factor (0.5/0.5) keeps both signals meaningful
        RRF_BLEND = 0.5    # weight of RRF component in final score
        COSINE_BLEND = 0.5  # weight of cosine component in final score

        all_keys = set(fts5_ranked.keys()) | set(sem_ranked.keys())
        scored = {}
        for key in all_keys:
            rrf_score = 0.0
            cosine_score = 0.0
            sources = []

            if key in fts5_ranked:
                rrf_score += fts5_weight * (1.0 / (k + fts5_ranked[key]['rank'] + 1))
                sources.append('exact')

            if key in sem_ranked:
                rrf_score += semantic_weight * (1.0 / (k + sem_ranked[key]['rank'] + 1))
                cosine_score = sem_ranked[key]['cosine'] / max_cosine  # normalize to 0-1
                sources.append('semantic')

            # Blend: RRF for agreement, cosine for discrimination
            # Scale RRF to roughly 0-1 range (max RRF with k=60 ≈ 0.033)
            rrf_normalized = rrf_score / 0.033
            final_score = RRF_BLEND * rrf_normalized + COSINE_BLEND * cosine_score

            scored[key] = {
                'score': final_score,
                'cosine': sem_ranked[key]['cosine'] if key in sem_ranked else 0.0,
                'sources': sources,
            }

        # 5. Build result objects
        results = []
        for key, s in scored.items():
            if key in fts5_ranked:
                data = dict(fts5_ranked[key]['data'])
            else:
                data = self._enrich_semantic_result(
                    sem_ranked[key]['data'], key
                )
            data['score'] = s['score']
            data['cosine'] = s['cosine']
            data['source'] = 'hybrid' if len(s['sources']) > 1 else s['sources'][0]
            results.append(data)

        # 6. Smart re-ranking (only above 50% of max score)
        if results:
            max_score = max(r['score'] for r in results)
            threshold = max_score * 0.5
            for r in results:
                if r['score'] >= threshold:
                    if r['type'] == 'function':
                        r['score'] *= 1.10
                    elif r['type'] == 'change':
                        r['score'] *= 0.95
                    if r['source'] == 'hybrid':
                        r['score'] *= 1.15

        # 7. Sort by score
        results.sort(key=lambda x: x['score'], reverse=True)

        # 8. Minimum score cutoff — don't return noise
        if results:
            top_score = results[0]['score']
            min_cutoff = top_score * 0.15  # results below 15% of top are noise
            results = [r for r in results if r['score'] >= min_cutoff]

        # 9. Diversity: max 3 per file
        final = []
        file_counts = {}
        for r in results:
            fp = r.get('file_path', '')
            if fp:
                file_counts[fp] = file_counts.get(fp, 0) + 1
                if file_counts[fp] > 3:
                    continue
            final.append(r)
            if len(final) >= limit:
                break

        # 10. Relevance labels (calibrated for RRF+cosine blend, 0-1 range)
        for r in final:
            s = r['score']
            if s >= 0.70:
                r['relevance'] = 'HIGH'
            elif s >= 0.50:
                r['relevance'] = 'GOOD'
            elif s >= 0.30:
                r['relevance'] = 'FAIR'
            elif s >= 0.15:
                r['relevance'] = 'LOW'
            else:
                r['relevance'] = 'WEAK'

        return final

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()


RENAME_THRESHOLD = 0.65


class GitMiningEngine:
    """Extracts git history and metadata from a repository."""

    SUPPORTED_EXTENSIONS = {
        '.py': 'python',
        '.js': 'javascript',
        '.ts': 'typescript',
        '.tsx': 'typescript',
        '.jsx': 'javascript',
        '.rs': 'rust',
        '.go': 'go',
        '.java': 'java',
        '.c': 'c',
        '.h': 'c',
        '.cpp': 'cpp',
        '.cc': 'cpp',
        '.cxx': 'cpp',
        '.hpp': 'cpp',
    }

    def __init__(self, repo_path: str):
        self.repo_path = os.path.abspath(repo_path)
        self.repo = Repo(repo_path)
        self._stats_cache: Dict[str, Dict] = {}

    def get_repo_name(self) -> str:
        return os.path.basename(self.repo_path)

    def extract_commits(self, max_count: Optional[int] = None) -> List[GitCommit]:
        """Extract commit history from the repository."""
        commits = []
        kwargs = {}
        if max_count is not None:
            kwargs['max_count'] = max_count

        for commit in self.repo.iter_commits('HEAD', **kwargs):
            # Cache stats for later reuse in get_changed_files
            stats_files = dict(commit.stats.files)
            self._stats_cache[commit.hexsha] = stats_files
            gc = GitCommit(
                hash=commit.hexsha,
                author=commit.author.name,
                timestamp=datetime.fromtimestamp(commit.authored_date),
                message=commit.message.strip(),
                files_changed=len(stats_files),
            )
            commits.append(gc)
        return commits

    def get_file_at_commit(self, commit_hash: str, file_path: str) -> Optional[str]:
        """Get file contents at a specific commit."""
        try:
            commit = self.repo.commit(commit_hash)
            blob = commit.tree / file_path
            return blob.data_stream.read().decode('utf-8', errors='replace')
        except KeyError:
            return None

    def _get_stats_files(self, commit_hash: str) -> Dict:
        """Get commit.stats.files with caching to avoid redundant git subprocess calls."""
        if commit_hash not in self._stats_cache:
            self._stats_cache[commit_hash] = dict(self.repo.commit(commit_hash).stats.files)
        return self._stats_cache[commit_hash]

    def get_changed_files(self, commit_hash: str) -> List[Dict]:
        """Get files changed in a commit with diff stats."""
        changes = []
        for path, stats in self._get_stats_files(commit_hash).items():
            changes.append({
                'path': path,
                'insertions': stats.get('insertions', 0),
                'deletions': stats.get('deletions', 0),
                'lines': stats.get('lines', 0),
            })
        return changes

    def get_blame_for_file(self, file_path: str) -> List[Tuple[str, str, int]]:
        """Run git blame on a file. Returns list of (commit_hash, author, line_number)."""
        try:
            blame = self.repo.blame('HEAD', file_path)
        except git.GitCommandError:
            return []

        results = []
        line_num = 1
        for commit, lines in blame:
            for _ in lines:
                results.append((commit.hexsha, commit.author.name, line_num))
                line_num += 1
        return results

    def get_tracked_files(self) -> List[str]:
        """Get all tracked files with supported extensions."""
        files = []
        for item in self.repo.tree().traverse():
            if item.type == 'blob':
                ext = os.path.splitext(item.path)[1]
                if ext in self.SUPPORTED_EXTENSIONS:
                    files.append(item.path)
        return files

    def get_files_changed_between(self, old_commit: str, new_commit: str) -> List[str]:
        """Return file paths changed between two commits, filtered to supported extensions."""
        diff = self.repo.commit(old_commit).diff(self.repo.commit(new_commit))
        files = set()
        for d in diff:
            for p in (d.a_path, d.b_path):
                if p:
                    ext = os.path.splitext(p)[1]
                    if ext in self.SUPPORTED_EXTENSIONS:
                        files.add(p)
        return sorted(files)

    def is_ancestor(self, ancestor_hash: str, descendant_hash: str) -> bool:
        """Check if ancestor_hash is an ancestor of descendant_hash."""
        try:
            return self.repo.is_ancestor(ancestor_hash, descendant_hash)
        except git.GitCommandError:
            return False

    def get_parent_hash(self, commit_hash: str) -> Optional[str]:
        """Get the first parent commit hash, or None for root commits."""
        commit = self.repo.commit(commit_hash)
        if commit.parents:
            return commit.parents[0].hexsha
        return None

    def _rename_score(self, old_func, new_func, before_lines, after_lines):
        """Score similarity between deleted and added function. Returns (score, signals_dict)."""
        import difflib

        # Must be same kind
        if old_func.kind != new_func.kind:
            return 0.0, {}

        # Extract bodies
        old_body = '\n'.join(before_lines[old_func.start_line - 1:old_func.end_line])
        new_body = '\n'.join(after_lines[new_func.start_line - 1:new_func.end_line])

        # Signals
        body_sim = difflib.SequenceMatcher(None, old_body, new_body).ratio()
        sig_sim = difflib.SequenceMatcher(None, old_func.signature, new_func.signature).ratio()
        name_sim = difflib.SequenceMatcher(None, old_func.name, new_func.name).ratio()

        max_lines = max(old_func.line_count, new_func.line_count, 1)
        size_sim = 1.0 - abs(old_func.line_count - new_func.line_count) / max_lines

        # Location
        if old_func.file_path == new_func.file_path:
            loc_sim = 1.0
        elif old_func.file_path.rsplit('/', 1)[0] == new_func.file_path.rsplit('/', 1)[0]:
            loc_sim = 0.3
        else:
            loc_sim = 0.0

        signals = {'body': body_sim, 'signature': sig_sim, 'name': name_sim, 'size': size_sim, 'location': loc_sim}

        score = body_sim * 0.45 + sig_sim * 0.20 + loc_sim * 0.15 + size_sim * 0.10 + name_sim * 0.10
        return round(score, 3), signals

    def _detect_renames(self, deleted_funcs, added_funcs, before_lines, after_lines):
        """Greedy 1:1 matching of deleted->added pairs above RENAME_THRESHOLD.
        Returns list of (old_func, new_func, score, signals) tuples."""
        candidates = []
        for old_func in deleted_funcs:
            for new_func in added_funcs:
                score, signals = self._rename_score(old_func, new_func, before_lines, after_lines)
                if score >= RENAME_THRESHOLD:
                    candidates.append((old_func, new_func, score, signals))

        # Sort by score descending for greedy matching
        candidates.sort(key=lambda x: x[2], reverse=True)

        matched = []
        used_old = set()
        used_new = set()
        for old_func, new_func, score, signals in candidates:
            old_key = (old_func.name, old_func.file_path, old_func.kind)
            new_key = (new_func.name, new_func.file_path, new_func.kind)
            if old_key not in used_old and new_key not in used_new:
                used_old.add(old_key)
                used_new.add(new_key)
                matched.append((old_func, new_func, score, signals))

        return matched

    def detect_function_changes(self, functions_before: List[FunctionInfo],
                                 functions_after: List[FunctionInfo],
                                 source_before: str = "", source_after: str = "") -> List[Dict]:
        """Compare function lists between two versions to detect changes.
        If source strings are provided, also compares function body content.
        Returns list of dicts with 'name', 'change_type', 'func' keys."""
        before_map = {(f.name, f.file_path, f.kind): f for f in functions_before}
        after_map = {(f.name, f.file_path, f.kind): f for f in functions_after}
        before_lines = source_before.split('\n') if source_before else []
        after_lines = source_after.split('\n') if source_after else []

        changes = []
        added = []
        deleted = []

        for key, func in after_map.items():
            if key not in before_map:
                added.append(func)
            else:
                old = before_map[key]
                modified = False
                if old.start_line != func.start_line or old.end_line != func.end_line or old.signature != func.signature:
                    modified = True
                elif before_lines and after_lines:
                    # Compare actual body content
                    old_body = before_lines[old.start_line - 1:old.end_line]
                    new_body = after_lines[func.start_line - 1:func.end_line]
                    if old_body != new_body:
                        modified = True
                if modified:
                    changes.append({'name': func.name, 'change_type': 'modified', 'func': func})

        for key, func in before_map.items():
            if key not in after_map:
                deleted.append(func)

        # Detect renames among deleted/added pairs
        renames = []
        if deleted and added and before_lines and after_lines:
            renames = self._detect_renames(deleted, added, before_lines, after_lines)

        renamed_old = {(r[0].name, r[0].file_path, r[0].kind) for r in renames}
        renamed_new = {(r[1].name, r[1].file_path, r[1].kind) for r in renames}

        for old_func, new_func, score, signals in renames:
            changes.append({
                'name': new_func.name, 'change_type': 'renamed', 'func': new_func,
                'old_name': old_func.name, 'old_file_path': old_func.file_path,
                'old_kind': old_func.kind, 'confidence': score,
                'signals': signals,
            })

        for func in added:
            key = (func.name, func.file_path, func.kind)
            if key not in renamed_new:
                changes.append({'name': func.name, 'change_type': 'added', 'func': func})

        for func in deleted:
            key = (func.name, func.file_path, func.kind)
            if key not in renamed_old:
                changes.append({'name': func.name, 'change_type': 'deleted', 'func': func})

        return changes
