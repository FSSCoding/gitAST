"""Tests for GitAST core: DataStore and GitMiningEngine"""
import os
import shutil
import tempfile
import time
from datetime import datetime

import pytest

from gitast.core import DataStore, GitMiningEngine, parse_date_filter, RENAME_THRESHOLD
from gitast.models import GitCommit, FunctionInfo, BlameEntry, FunctionChange


class TestParseDateFilter:
    def test_iso_date(self):
        ts = parse_date_filter("2026-01-01")
        expected = int(datetime(2026, 1, 1).timestamp())
        assert ts == expected

    def test_relative_days(self):
        ts = parse_date_filter("30d")
        now = int(time.time())
        expected = now - 30 * 86400
        assert abs(ts - expected) < 2  # allow 1s drift

    def test_relative_months(self):
        ts = parse_date_filter("6m")
        now = int(time.time())
        expected = now - 6 * 30 * 86400
        assert abs(ts - expected) < 2

    def test_relative_years(self):
        ts = parse_date_filter("1y")
        now = int(time.time())
        expected = now - 365 * 86400
        assert abs(ts - expected) < 2

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_date_filter("not-a-date")


class TestDataStore:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, ".gitast", "index.db")

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        return store

    def test_schema_creation(self):
        store = self._make_store()
        tables = store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t['name'] for t in tables]
        assert 'commits' in table_names
        assert 'functions' in table_names
        assert 'function_changes' in table_names
        assert 'blame_entries' in table_names
        assert 'meta' in table_names
        store.close()

    def test_save_and_get_commit(self):
        store = self._make_store()
        commit = GitCommit(
            hash="abc123", author="Bob",
            timestamp=datetime(2025, 1, 1, 12, 0),
            message="Test commit", files_changed=2,
        )
        store.save_commit(commit)
        store.flush()

        result = store.get_commit("abc123")
        assert result is not None
        assert result.author == "Bob"
        assert result.message == "Test commit"
        store.close()

    def test_get_commit_not_found(self):
        store = self._make_store()
        result = store.get_commit("nonexistent")
        assert result is None
        store.close()

    def test_save_function(self):
        store = self._make_store()
        func = FunctionInfo(
            name="my_func", file_path="test.py", language="python",
            start_line=1, end_line=10, kind="function",
            signature="def my_func():",
        )
        store.save_function(func)
        store.flush()

        funcs = store.get_functions_in_file("test.py")
        assert len(funcs) == 1
        assert funcs[0].name == "my_func"
        store.close()

    def test_duplicate_function_names_different_scopes(self):
        """Same name at different line positions should not collide."""
        store = self._make_store()
        store.save_function(FunctionInfo(
            name="helper", file_path="app.py", language="python",
            start_line=10, end_line=15, kind="method",
        ))
        store.save_function(FunctionInfo(
            name="helper", file_path="app.py", language="python",
            start_line=30, end_line=35, kind="method",
        ))
        store.flush()

        funcs = store.get_functions_in_file("app.py")
        assert len(funcs) == 2
        store.close()

    def test_function_history(self):
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="aaa", author="A", timestamp=datetime(2025, 1, 1),
            message="m", files_changed=1,
        ))
        change = FunctionChange(
            function_name="do_thing", file_path="app.py",
            commit_hash="aaa", change_type="added",
            lines_added=10, author="A",
            timestamp=datetime(2025, 1, 1), message="Added do_thing",
        )
        store.save_function_change(change)
        store.flush()

        history = store.get_function_history("app.py", "do_thing")
        assert len(history) == 1
        assert history[0].change_type == "added"
        store.close()

    def test_blame_entries(self):
        store = self._make_store()
        entry = BlameEntry(
            function_name="parse", file_path="parser.py",
            author="Alice", line_count=15, percentage=75.0,
            commit_hash="xyz",
        )
        store.save_blame_entry(entry)
        store.flush()

        entries = store.get_function_blame("parser.py", "parse")
        assert len(entries) == 1
        assert entries[0].author == "Alice"
        assert entries[0].percentage == 75.0
        store.close()

    def test_stats(self):
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="a1", author="X", timestamp=datetime(2025, 1, 1),
            message="m", files_changed=1,
        ))
        store.save_function(FunctionInfo(
            name="f1", file_path="x.py", language="python",
            start_line=1, end_line=5, kind="function",
        ))
        store.flush()
        stats = store.get_stats()
        assert stats['commits'] == 1
        assert stats['functions'] == 1
        store.close()

    def test_context_manager(self):
        with DataStore(self.db_path) as store:
            store.create_schema()
            stats = store.get_stats()
            assert stats['commits'] == 0

    def test_bare_filename_path(self):
        """DataStore with bare filename (no directory) should not crash."""
        bare_path = os.path.join(self.tmp, "bare.db")
        store = DataStore(bare_path)
        store.connect()
        store.create_schema()
        stats = store.get_stats()
        assert stats['commits'] == 0
        store.close()

    # -- FTS5 tests --

    def test_fts_search(self):
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="aaa", author="Alice", timestamp=datetime(2025, 1, 1),
            message="Fix authentication timeout in API", files_changed=3,
        ))
        store.save_commit(GitCommit(
            hash="bbb", author="Bob", timestamp=datetime(2025, 1, 2),
            message="Add database migration script", files_changed=1,
        ))
        store.save_function(FunctionInfo(
            name="login", file_path="auth.py", language="python",
            start_line=1, end_line=20, kind="function",
            signature="def login(username, password):",
        ))
        store.flush()
        doc_count = store.rebuild_search_index()
        assert doc_count == 3

        results = store.search("authentication")
        assert len(results) > 0
        assert any("authentication" in r['content'].lower() for r in results)

        results = store.search("database")
        assert len(results) > 0

        results = store.search("login")
        assert len(results) > 0
        assert any(r['name'] == 'login' for r in results)
        store.close()

    def test_fts_no_results(self):
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="ccc", author="Eve", timestamp=datetime(2025, 1, 1),
            message="Initial commit", files_changed=1,
        ))
        store.flush()
        store.rebuild_search_index()
        results = store.search("xyznonexistent")
        assert results == []
        store.close()

    def test_fts_camelcase_splitting(self):
        """CamelCase identifiers should be searchable by individual words."""
        store = self._make_store()
        store.save_function(FunctionInfo(
            name="GitMiningEngine", file_path="core.py", language="python",
            start_line=1, end_line=50, kind="class",
            signature="class GitMiningEngine:",
        ))
        store.flush()
        store.rebuild_search_index()

        results = store.search("mining")
        assert len(results) > 0
        assert any("GitMiningEngine" in r['name'] or "mining" in r['content'].lower() for r in results)
        store.close()

    def test_fts_snake_case_splitting(self):
        """snake_case identifiers should be searchable by individual words."""
        store = self._make_store()
        store.save_function(FunctionInfo(
            name="get_blame_for_file", file_path="core.py", language="python",
            start_line=1, end_line=10, kind="function",
            signature="def get_blame_for_file(self, file_path):",
        ))
        store.flush()
        store.rebuild_search_index()

        results = store.search("blame")
        assert len(results) > 0
        store.close()

    def test_fts_author_populated_from_blame(self):
        """Function search results should include the primary author from blame data."""
        store = self._make_store()
        store.save_function(FunctionInfo(
            name="process", file_path="worker.py", language="python",
            start_line=1, end_line=20, kind="function",
        ))
        store.save_blame_entry(BlameEntry(
            function_name="process", file_path="worker.py",
            author="Alice", line_count=15, percentage=75.0,
        ))
        store.save_blame_entry(BlameEntry(
            function_name="process", file_path="worker.py",
            author="Bob", line_count=5, percentage=25.0,
        ))
        store.flush()
        store.rebuild_search_index()

        results = store.search("process")
        assert len(results) > 0
        func_result = next(r for r in results if r['type'] == 'function')
        assert func_result['author'] == 'Alice'
        store.close()

    def _make_store_with_changes(self):
        """Helper: store with commits, functions, blame, and changes populated."""
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="aaa", author="Alice", timestamp=datetime(2025, 1, 1),
            message="add stuff", files_changed=2,
        ))
        store.save_commit(GitCommit(
            hash="bbb", author="Bob", timestamp=datetime(2025, 2, 1),
            message="fix stuff", files_changed=1,
        ))
        store.save_function(FunctionInfo(
            name="process", file_path="core.py", language="python",
            start_line=1, end_line=20, kind="function",
        ))
        store.save_function(FunctionInfo(
            name="helper", file_path="core.py", language="python",
            start_line=22, end_line=30, kind="function",
        ))
        store.save_blame_entry(BlameEntry(
            function_name="process", file_path="core.py",
            author="Alice", line_count=18, percentage=90.0, commit_hash="aaa",
        ))
        store.save_blame_entry(BlameEntry(
            function_name="helper", file_path="core.py",
            author="Bob", line_count=9, percentage=100.0, commit_hash="bbb",
        ))
        for i, (ct, author, ts) in enumerate([
            ("added", "Alice", datetime(2025, 1, 1)),
            ("modified", "Bob", datetime(2025, 2, 1)),
            ("modified", "Alice", datetime(2025, 3, 1)),
        ]):
            store.save_function_change(FunctionChange(
                function_name="process", file_path="core.py",
                commit_hash="aaa", change_type=ct,
                author=author, timestamp=ts, message="change",
            ))
        store.save_function_change(FunctionChange(
            function_name="helper", file_path="core.py",
            commit_hash="bbb", change_type="added",
            author="Bob", timestamp=datetime(2025, 2, 1), message="add helper",
        ))
        store.flush()
        return store

    def test_get_hotspots_basic(self):
        store = self._make_store_with_changes()
        results = store.get_hotspots(limit=10)
        assert len(results) >= 1
        # process has 3 changes, helper has 1 - process should be first
        assert results[0]['function_name'] == 'process'
        assert results[0]['change_count'] == 3
        assert results[0]['added'] == 1
        assert results[0]['modified'] == 2
        store.close()

    def test_get_hotspots_author_filter(self):
        store = self._make_store_with_changes()
        results = store.get_hotspots(author="Bob")
        names = [r['function_name'] for r in results]
        # Bob touched process (modified) and helper (added)
        assert "process" in names or "helper" in names
        store.close()

    def test_get_hotspots_file_filter(self):
        store = self._make_store_with_changes()
        results = store.get_hotspots(file_filter="core.py")
        assert all('core.py' in r['file_path'] for r in results)
        store.close()

    def test_get_hotspots_empty(self):
        store = self._make_store()
        results = store.get_hotspots()
        assert results == []
        store.close()

    def test_get_file_blame_summary(self):
        store = self._make_store_with_changes()
        results = store.get_file_blame_summary("core.py")
        assert len(results) == 2
        names = [r['name'] for r in results]
        assert "process" in names
        assert "helper" in names
        # ordered by start_line
        assert results[0]['name'] == 'process'
        assert results[0]['primary_owner'] == 'Alice'
        assert results[0]['ownership_pct'] == 90.0
        assert results[0]['change_count'] == 3
        assert results[1]['name'] == 'helper'
        assert results[1]['primary_owner'] == 'Bob'
        store.close()

    def test_get_file_blame_summary_no_file(self):
        store = self._make_store()
        results = store.get_file_blame_summary("nonexistent.py")
        assert results == []
        store.close()

    def test_split_identifiers(self):
        """Test the static identifier splitting method."""
        result = DataStore._split_identifiers("GitMiningEngine")
        assert "mining" in result.lower()
        assert "engine" in result.lower()
        assert "git" in result.lower()

        result = DataStore._split_identifiers("get_blame_for_file")
        assert "blame" in result
        assert "file" in result

        result = DataStore._split_identifiers("simple")
        assert "simple" in result

    def test_split_identifiers_number_boundaries(self):
        """Number boundaries in identifiers should be split."""
        result = DataStore._split_identifiers("test123func")
        assert "test" in result.lower()
        assert "123" in result
        assert "func" in result.lower()

    def test_split_identifiers_no_trailing_space(self):
        """Plain text with no identifiers should not have trailing space."""
        result = DataStore._split_identifiers("hello")
        assert result == "hello"
        assert not result.endswith(' ')

    def test_double_connect_no_leak(self):
        """Calling connect() twice should be a no-op, not leak connections."""
        store = DataStore(self.db_path)
        store.connect()
        first_conn = store.conn
        store.connect()
        assert store.conn is first_conn
        store.close()

    def test_clear_all(self):
        """clear_all should remove all data."""
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="a1", author="X", timestamp=datetime(2025, 1, 1),
            message="m", files_changed=1,
        ))
        store.flush()
        assert store.get_stats()['commits'] == 1
        store.clear_all()
        assert store.get_stats()['commits'] == 0
        store.close()

    def test_search_empty_query(self):
        """Empty or whitespace query should return empty list."""
        store = self._make_store()
        assert store.search("") == []
        assert store.search("   ") == []
        store.close()

    def test_search_special_chars(self):
        """Query with only special chars should not crash."""
        store = self._make_store()
        assert store.search("!@#$%^&*()") == []
        store.close()

    def test_sanitize_fts_query(self):
        """FTS query sanitizer should extract words and quote them."""
        assert DataStore._sanitize_fts_query("hello world") == '"hello" "world"'
        assert DataStore._sanitize_fts_query("NOT OR AND") == '"NOT" "OR" "AND"'
        assert DataStore._sanitize_fts_query("") is None
        assert DataStore._sanitize_fts_query("!!!") is None
        assert DataStore._sanitize_fts_query("it's a test") == '"it" "s" "a" "test"'


    # -- v0.2 meta + incremental tests --

    def test_get_set_meta(self):
        store = self._make_store()
        assert store.get_meta('nonexistent') is None
        store.set_meta('last_indexed_commit', 'abc123')
        assert store.get_meta('last_indexed_commit') == 'abc123'
        # Overwrite
        store.set_meta('last_indexed_commit', 'def456')
        assert store.get_meta('last_indexed_commit') == 'def456'
        store.close()

    def test_get_indexed_commit_hashes(self):
        store = self._make_store_with_changes()
        hashes = store.get_indexed_commit_hashes()
        assert 'aaa' in hashes
        assert 'bbb' in hashes
        assert len(hashes) == 2
        store.close()

    def test_get_indexed_commit_hashes_empty(self):
        store = self._make_store()
        hashes = store.get_indexed_commit_hashes()
        assert hashes == set()
        store.close()

    def test_delete_file_data(self):
        store = self._make_store_with_changes()
        # Verify data exists
        funcs = store.get_functions_in_file("core.py")
        assert len(funcs) == 2
        blame = store.get_function_blame("core.py", "process")
        assert len(blame) >= 1

        # Delete
        store.delete_file_data("core.py")
        store.flush()

        # Verify deleted
        funcs = store.get_functions_in_file("core.py")
        assert len(funcs) == 0
        blame = store.get_function_blame("core.py", "process")
        assert len(blame) == 0

        # function_changes should still exist (not deleted by delete_file_data)
        changes = store.get_function_history("core.py", "process")
        assert len(changes) >= 1
        store.close()

    def test_delete_file_data_nonexistent(self):
        store = self._make_store()
        # Should not crash
        store.delete_file_data("nonexistent.py")
        store.close()

    # -- v0.3 query tests --

    def test_get_functions_by_pattern(self):
        store = self._make_store_with_changes()
        results = store.get_functions_by_pattern("proc")
        assert len(results) == 1
        assert results[0]['name'] == 'process'
        store.close()

    def test_get_functions_by_pattern_kind_filter(self):
        store = self._make_store_with_changes()
        results = store.get_functions_by_pattern("proc", kind="function")
        assert len(results) == 1
        results = store.get_functions_by_pattern("proc", kind="class")
        assert results == []
        store.close()

    def test_get_functions_by_pattern_file_filter(self):
        store = self._make_store_with_changes()
        results = store.get_functions_by_pattern("proc", file_filter="core.py")
        assert len(results) == 1
        results = store.get_functions_by_pattern("proc", file_filter="other.py")
        assert results == []
        store.close()

    def test_get_functions_by_pattern_no_match(self):
        store = self._make_store_with_changes()
        results = store.get_functions_by_pattern("nonexistent_xyz")
        assert results == []
        store.close()

    def test_get_functions_by_pattern_limit(self):
        store = self._make_store_with_changes()
        results = store.get_functions_by_pattern("", limit=1)
        assert len(results) == 1
        store.close()

    def test_get_function_ages(self):
        store = self._make_store_with_changes()
        results = store.get_function_ages()
        assert len(results) >= 2
        names = [r['name'] for r in results]
        assert 'helper' in names
        assert 'process' in names
        for r in results:
            assert r['days_ago'] >= 0 or r['days_ago'] == -1
        store.close()

    def test_get_function_ages_recent_first(self):
        store = self._make_store_with_changes()
        results = store.get_function_ages(recent_first=True)
        assert len(results) >= 2
        assert results[0]['name'] == 'process'
        store.close()

    def test_get_function_ages_file_filter(self):
        store = self._make_store_with_changes()
        results = store.get_function_ages(file_filter="core.py")
        assert len(results) >= 1
        results_none = store.get_function_ages(file_filter="other.py")
        assert results_none == []
        store.close()

    def test_get_timeline(self):
        store = self._make_store_with_changes()
        results = store.get_timeline(months=24)
        assert len(results) >= 1
        for r in results:
            assert 'month' in r
            assert 'commits' in r
            assert 'changes' in r
        store.close()

    def test_get_timeline_empty(self):
        store = self._make_store()
        results = store.get_timeline()
        assert results == []
        store.close()

    def test_get_commit_diff_single(self):
        store = self._make_store_with_changes()
        results = store.get_commit_diff("aaa")
        assert len(results) >= 1
        assert all(r['commit_hash'] == 'aaa' for r in results)
        store.close()

    def test_get_commit_diff_prefix(self):
        store = self._make_store_with_changes()
        results = store.get_commit_diff("aa")
        assert len(results) >= 1
        store.close()

    def test_get_commit_diff_no_match(self):
        store = self._make_store_with_changes()
        results = store.get_commit_diff("zzz_nonexistent")
        assert results == []
        store.close()

    def test_get_commit_diff_range(self):
        store = self._make_store_with_changes()
        results = store.get_commit_diff("aaa", "bbb")
        assert len(results) >= 1
        store.close()

    def test_get_file_report(self):
        store = self._make_store_with_changes()
        report = store.get_file_report("core.py")
        assert report is not None
        assert report['file_path'] == 'core.py'
        assert report['total_functions'] == 2
        assert report['total_changes'] == 4
        assert len(report['functions']) == 2
        proc = next(f for f in report['functions'] if f['name'] == 'process')
        assert proc['owner'] == 'Alice'
        assert proc['change_count'] == 3
        store.close()

    def test_get_file_report_fuzzy(self):
        store = self._make_store_with_changes()
        report = store.get_file_report("core")
        assert report is not None
        assert 'core.py' in report['file_path']
        store.close()

    def test_get_file_report_not_found(self):
        store = self._make_store_with_changes()
        report = store.get_file_report("nonexistent_xyz.py")
        assert report is None
        store.close()

    def test_get_release_diff_summary(self):
        store = self._make_store_with_changes()
        # All changes up to commit "bbb"
        summary = store.get_release_diff_summary(None, "bbb")
        assert summary['total'] == 4  # all changes
        assert summary['added'] == 2  # process + helper
        assert summary['modified'] == 2
        assert summary['deleted'] == 0
        assert summary['files_touched'] == 1  # only core.py
        assert summary['authors'] == 2  # Alice + Bob
        store.close()

    def test_get_release_diff_summary_partial_range(self):
        store = self._make_store_with_changes()
        # Changes after "aaa" up to "bbb" — only commit "bbb" (helper added by Bob)
        # Note: test data reuses commit_hash="aaa" for multiple changes,
        # so only the "bbb" commit change falls in this range
        summary = store.get_release_diff_summary("aaa", "bbb")
        assert summary['total'] == 1
        assert summary['added'] == 1  # helper added
        assert summary['authors'] == 1  # Bob only
        store.close()

    def test_get_release_diff_summary_empty_range(self):
        store = self._make_store_with_changes()
        # Non-existent hash returns empty
        summary = store.get_release_diff_summary("bbb", "zzz_nonexistent")
        assert summary['total'] == 0
        assert summary['added'] == 0
        assert summary['files_touched'] == 0
        store.close()

    def test_save_and_get_config_history(self):
        from gitast.models import ConfigChange
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="ccc", author="Alice", timestamp=datetime(2025, 3, 1),
            message="update config", files_changed=1,
        ))
        store.save_config_change(ConfigChange(
            file_path="config.json", key_path="db.host",
            commit_hash="ccc", change_type="modified",
            old_value="localhost", new_value="prod.example.com",
            author="Alice", timestamp=datetime(2025, 3, 1),
            message="update config",
        ))
        store.save_config_change(ConfigChange(
            file_path="config.json", key_path="db.port",
            commit_hash="ccc", change_type="added",
            old_value=None, new_value="5432",
            author="Alice", timestamp=datetime(2025, 3, 1),
            message="update config",
        ))
        store.flush()

        results = store.get_config_history("db.host")
        assert len(results) == 1
        assert results[0]['old_value'] == "localhost"
        assert results[0]['new_value'] == "prod.example.com"
        assert results[0]['change_type'] == "modified"

        results = store.get_config_history("db")
        assert len(results) == 2

        results = store.get_config_history("db.host", file_path="config.json")
        assert len(results) == 1

        results = store.get_config_history("nonexistent")
        assert len(results) == 0
        store.close()

    def test_get_config_keys(self):
        from gitast.models import ConfigChange
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="ddd", author="Bob", timestamp=datetime(2025, 4, 1),
            message="config change", files_changed=1,
        ))
        for key in ["db.host", "db.host", "db.port"]:
            store.save_config_change(ConfigChange(
                file_path="config.json", key_path=key,
                commit_hash="ddd", change_type="modified",
                author="Bob", timestamp=datetime(2025, 4, 1),
                message="change",
            ))
        store.flush()

        keys = store.get_config_keys()
        assert len(keys) == 2
        # db.host has 2 changes, should be first
        assert keys[0]['key_path'] == 'db.host'
        assert keys[0]['change_count'] == 2

        keys = store.get_config_keys(file_filter="config.json")
        assert len(keys) == 2

        keys = store.get_config_keys(file_filter="nonexistent")
        assert len(keys) == 0
        store.close()

    def test_save_and_get_dep_history(self):
        from gitast.models import DepChange
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="eee", author="Alice", timestamp=datetime(2025, 5, 1),
            message="bump deps", files_changed=1,
        ))
        store.save_dep_change(DepChange(
            file_path="requirements.txt", package="requests",
            commit_hash="eee", change_type="bumped",
            old_version="==2.28.0", new_version="==2.31.0",
            author="Alice", timestamp=datetime(2025, 5, 1),
            message="bump deps",
        ))
        store.save_dep_change(DepChange(
            file_path="requirements.txt", package="flask",
            commit_hash="eee", change_type="added",
            old_version=None, new_version=">=2.0",
            author="Alice", timestamp=datetime(2025, 5, 1),
            message="bump deps",
        ))
        store.flush()

        results = store.get_dep_history(package="requests")
        assert len(results) == 1
        assert results[0]['change_type'] == "bumped"
        assert results[0]['old_version'] == "==2.28.0"

        results = store.get_dep_history()
        assert len(results) == 2

        results = store.get_dep_history(change_type="added")
        assert len(results) == 1
        assert results[0]['package'] == "flask"

        results = store.get_dep_history(package="nonexistent")
        assert len(results) == 0
        store.close()

    def test_get_dep_summary(self):
        from gitast.models import DepChange
        store = self._make_store()
        store.save_commit(GitCommit(
            hash="fff", author="Bob", timestamp=datetime(2025, 6, 1),
            message="deps", files_changed=1,
        ))
        for pkg, ct in [("requests", "bumped"), ("requests", "bumped"), ("flask", "added")]:
            store.save_dep_change(DepChange(
                file_path="requirements.txt", package=pkg,
                commit_hash="fff", change_type=ct,
                author="Bob", timestamp=datetime(2025, 6, 1),
                message="deps",
            ))
        store.flush()

        summary = store.get_dep_summary()
        assert len(summary) == 2
        assert summary[0]['package'] == 'requests'
        assert summary[0]['change_count'] == 2
        assert summary[0]['bumps'] == 2
        store.close()


class TestExportData:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, 'test.db')

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        store.save_commit(GitCommit("aaa", "Alice", datetime(2023, 11, 14), "fix bug", 2))
        store.save_function(FunctionInfo("my_func", "app.py", "python", 1, 10, "function", "def my_func():"))
        store.save_function_change(FunctionChange(
            "my_func", "app.py", "aaa", "added", 10, 0, "Alice", datetime(2023, 11, 14), "fix bug"))
        store.save_blame_entry(BlameEntry("my_func", "app.py", "Alice", 10, 100.0, "aaa"))
        store.flush()
        return store

    def test_export_all_sections(self):
        store = self._make_store()
        data = store.get_export_data()
        assert 'functions' in data
        assert 'changes' in data
        assert 'blame' in data
        assert len(data['functions']) == 1
        assert data['functions'][0]['name'] == 'my_func'
        store.close()

    def test_export_single_section(self):
        store = self._make_store()
        data = store.get_export_data(['functions'])
        assert 'functions' in data
        assert 'changes' not in data
        store.close()

    def test_export_empty_db(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        data = store.get_export_data(['functions'])
        assert data['functions'] == []
        store.close()


class TestStabilityScores:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, 'test.db')

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        from datetime import timedelta
        now = datetime.now()
        # Insert commits first (FK constraint)
        store.save_commit(GitCommit("aaa", "Alice", now - timedelta(days=300), "initial", 1))
        for i in range(10):
            store.save_commit(GitCommit(f"c{i:03d}", f"Author{i % 3}", now - timedelta(days=i), f"change {i}", 1))
        # Insert functions (needed for stability query)
        store.save_function(FunctionInfo("stable_func", "core.py", "python", 1, 20, "function"))
        store.save_function(FunctionInfo("volatile_func", "api.py", "python", 1, 30, "function"))
        # Stable function: 1 change, old
        store.save_function_change(FunctionChange(
            "stable_func", "core.py", "aaa", "added", 10, 0, "Alice",
            now - timedelta(days=300), "initial"))
        # Volatile function: many changes, recent
        for i in range(10):
            store.save_function_change(FunctionChange(
                "volatile_func", "api.py", f"c{i:03d}", "modified", 5, 2,
                f"Author{i % 3}", now - timedelta(days=i), f"change {i}"))
        store.flush()
        return store

    def test_stability_scores_ranked(self):
        store = self._make_store()
        scores = store.get_stability_scores()
        assert len(scores) == 2
        # Stable should come first (higher score)
        assert scores[0]['function_name'] == 'stable_func'
        assert scores[0]['stability_score'] > scores[1]['stability_score']
        store.close()

    def test_stability_ratings(self):
        store = self._make_store()
        scores = store.get_stability_scores()
        ratings = {s['function_name']: s['rating'] for s in scores}
        assert ratings['stable_func'] in ('stable', 'moderate')
        assert ratings['volatile_func'] in ('volatile', 'critical')
        store.close()

    def test_stability_empty(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        scores = store.get_stability_scores()
        assert scores == []
        store.close()

    def test_stability_includes_zero_change_functions(self):
        store = self._make_store()
        # Add a function with zero changes
        store.save_function(FunctionInfo("untouched_func", "lib.py", "python", 1, 10, "function"))
        store.flush()
        scores = store.get_stability_scores(limit=100)
        names = {s['function_name'] for s in scores}
        assert 'untouched_func' in names
        # Zero-change function should be the most stable
        untouched = next(s for s in scores if s['function_name'] == 'untouched_func')
        assert untouched['stability_score'] == 1.0
        assert untouched['rating'] == 'stable'
        assert untouched['change_count'] == 0
        store.close()

    def test_stability_file_filter(self):
        store = self._make_store()
        scores = store.get_stability_scores(file_filter='core')
        assert len(scores) == 1
        assert scores[0]['function_name'] == 'stable_func'
        store.close()


class TestNewQueryMethods:
    """Tests for v0.3.3 DataStore query methods."""

    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, 'test.db')

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        # Commits across 3 months
        store.save_commit(GitCommit("a1", "Alice", datetime(2025, 1, 15), "Add parser", 2))
        store.save_commit(GitCommit("a2", "Bob", datetime(2025, 1, 20), "Fix timeout", 1))
        store.save_commit(GitCommit("b1", "Alice", datetime(2025, 2, 10), "Refactor core", 3))
        store.save_commit(GitCommit("c1", "Alice", datetime(2025, 3, 5), "Add search", 2))
        store.save_commit(GitCommit("c2", "Bob", datetime(2025, 3, 15), "Implement cache", 1))
        # Functions
        store.save_function(FunctionInfo("parse", "parser.py", "python", 1, 20, "function"))
        store.save_function(FunctionInfo("search", "search.py", "python", 1, 30, "function"))
        store.save_function(FunctionInfo("legacy", "old.py", "python", 1, 10, "function"))
        # Extra commits for FK references
        store.save_commit(GitCommit("a3", "Bob", datetime(2025, 2, 15), "Fix parser", 1))
        store.save_commit(GitCommit("a4", "Alice", datetime(2025, 3, 5), "Tweak parser", 1))
        store.save_commit(GitCommit("a5", "Bob", datetime(2025, 3, 15), "Final parser fix", 1))
        # Changes: parse is fragile (many modifications)
        for commit_hash, ct, author, ts in [
            ("a1", "added", "Alice", datetime(2025, 1, 15)),
            ("a2", "modified", "Bob", datetime(2025, 1, 20)),
            ("b1", "modified", "Alice", datetime(2025, 2, 10)),
            ("a3", "modified", "Bob", datetime(2025, 2, 15)),
            ("a4", "modified", "Alice", datetime(2025, 3, 5)),
            ("a5", "modified", "Bob", datetime(2025, 3, 15)),
        ]:
            store.save_function_change(FunctionChange(
                "parse", "parser.py", commit_hash, ct, 5, 2,
                author, ts, f"change {commit_hash}"))
        # search is added once
        store.save_function_change(FunctionChange(
            "search", "search.py", "c1", "added", 30, 0,
            "Alice", datetime(2025, 3, 5), "Add search"))
        store.flush()
        return store

    def test_get_commits_by_month(self):
        store = self._make_store()
        result = store.get_commits_by_month()
        assert len(result) >= 3
        months = [r['month'] for r in result]
        assert '2025-01' in months
        assert '2025-02' in months
        assert '2025-03' in months
        jan = next(r for r in result if r['month'] == '2025-01')
        assert len(jan['commits']) == 2
        assert jan['commits'][0]['message'] == 'Add parser'
        store.close()

    def test_get_fragile_functions(self):
        store = self._make_store()
        result = store.get_fragile_functions()
        # parse has 6 changes, 5 modifications — should qualify
        names = [r['function_name'] for r in result]
        assert 'parse' in names
        parse = next(r for r in result if r['function_name'] == 'parse')
        assert parse['change_count'] == 6
        assert parse['modify_count'] == 5
        assert parse['author_count'] == 2
        # search should NOT be fragile (only 1 change)
        assert 'search' not in names
        store.close()

    def test_get_fragile_functions_empty(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        result = store.get_fragile_functions()
        assert result == []
        store.close()

    def test_get_stale_functions(self):
        store = self._make_store()
        result = store.get_stale_functions()
        assert len(result) >= 1
        # legacy has no changes — should be first (most stale)
        assert result[0]['function_name'] == 'legacy'
        assert result[0]['last_changed'] is None
        assert result[0]['total_changes'] == 0
        store.close()

    def test_get_stale_functions_limit(self):
        store = self._make_store()
        result = store.get_stale_functions(limit=1)
        assert len(result) == 1
        store.close()

    def test_get_coauthorship_patterns(self):
        store = self._make_store()
        result = store.get_coauthorship_patterns()
        # parse has 2 authors — should appear
        names = [r['function_name'] for r in result]
        assert 'parse' in names
        parse = next(r for r in result if r['function_name'] == 'parse')
        assert parse['author_count'] == 2
        assert 'Alice' in parse['authors']
        assert 'Bob' in parse['authors']
        # search has 1 author — should NOT appear
        assert 'search' not in names
        store.close()

    def test_get_coauthorship_empty(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        result = store.get_coauthorship_patterns()
        assert result == []
        store.close()

    def test_get_feature_expansion(self):
        store = self._make_store()
        result = store.get_feature_expansion()
        assert len(result) >= 1
        # January has 1 added function (parse)
        jan = next((r for r in result if r['month'] == '2025-01'), None)
        assert jan is not None
        assert jan['new_functions'] == 1
        assert jan['cumulative_functions'] >= 1
        store.close()

    def test_get_feature_expansion_empty(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        result = store.get_feature_expansion()
        assert result == []
        store.close()


class TestGitMiningEngine:
    def _get_engine(self):
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return GitMiningEngine(repo_root)

    def test_init_on_current_repo(self):
        engine = self._get_engine()
        assert engine.repo is not None
        assert "gitast" in engine.get_repo_name()

    def test_extract_commits(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=5)
        assert len(commits) > 0
        assert all(isinstance(c, GitCommit) for c in commits)

    def test_get_tracked_files(self):
        engine = self._get_engine()
        files = engine.get_tracked_files()
        py_files = [f for f in files if f.endswith('.py')]
        assert len(py_files) > 0  # Should have committed files now

    def test_get_file_at_commit(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=1)
        assert len(commits) > 0
        # Try to get pyproject.toml at HEAD
        content = engine.get_file_at_commit(commits[0].hash, "pyproject.toml")
        assert content is not None
        assert "gitast" in content

    def test_get_file_at_commit_nonexistent(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=1)
        content = engine.get_file_at_commit(commits[0].hash, "nonexistent_file_xyz.py")
        assert content is None

    def test_get_changed_files(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=1)
        changes = engine.get_changed_files(commits[0].hash)
        assert isinstance(changes, list)
        if changes:
            assert 'path' in changes[0]
            assert 'insertions' in changes[0]

    def test_get_blame_for_file(self):
        engine = self._get_engine()
        blame = engine.get_blame_for_file("setup.py")
        assert isinstance(blame, list)
        if blame:
            assert len(blame[0]) == 3  # (hash, author, line_num)

    def test_get_blame_nonexistent_file(self):
        engine = self._get_engine()
        blame = engine.get_blame_for_file("nonexistent_xyz.py")
        assert blame == []

    def test_get_parent_hash(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=5)
        if len(commits) >= 2:
            # Second commit (older) should have a parent or not
            parent = engine.get_parent_hash(commits[0].hash)
            # The most recent commit should have a parent if there are >1 commits
            assert parent is not None

    def test_get_parent_hash_root_commit(self):
        engine = self._get_engine()
        commits = engine.extract_commits()
        if commits:
            root = commits[-1]  # Oldest commit
            parent = engine.get_parent_hash(root.hash)
            # Root commit may or may not have a parent depending on repo
            # Just ensure it doesn't crash
            assert parent is None or isinstance(parent, str)

    def test_detect_function_changes_added(self):
        engine = self._get_engine()
        before = []
        after = [
            FunctionInfo(name="new_func", file_path="a.py", language="python",
                        start_line=1, end_line=5, kind="function"),
        ]
        changes = engine.detect_function_changes(before, after)
        assert len(changes) == 1
        assert changes[0]['change_type'] == 'added'
        assert changes[0]['name'] == 'new_func'

    def test_detect_function_changes_deleted(self):
        engine = self._get_engine()
        before = [
            FunctionInfo(name="old_func", file_path="a.py", language="python",
                        start_line=1, end_line=5, kind="function"),
        ]
        after = []
        changes = engine.detect_function_changes(before, after)
        assert len(changes) == 1
        assert changes[0]['change_type'] == 'deleted'

    def test_detect_function_changes_modified(self):
        engine = self._get_engine()
        before = [
            FunctionInfo(name="func", file_path="a.py", language="python",
                        start_line=1, end_line=5, kind="function", signature="def func():"),
        ]
        after = [
            FunctionInfo(name="func", file_path="a.py", language="python",
                        start_line=1, end_line=10, kind="function", signature="def func(x):"),
        ]
        changes = engine.detect_function_changes(before, after)
        assert len(changes) == 1
        assert changes[0]['change_type'] == 'modified'

    def test_detect_function_changes_no_change(self):
        engine = self._get_engine()
        func = FunctionInfo(name="func", file_path="a.py", language="python",
                           start_line=1, end_line=5, kind="function", signature="def func():")
        changes = engine.detect_function_changes([func], [func])
        assert len(changes) == 0

    def test_get_files_changed_between(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=3)
        if len(commits) >= 2:
            files = engine.get_files_changed_between(commits[1].hash, commits[0].hash)
            assert isinstance(files, list)
            # Should be sorted
            assert files == sorted(files)

    def test_is_ancestor(self):
        engine = self._get_engine()
        commits = engine.extract_commits(max_count=3)
        if len(commits) >= 2:
            # Older commit is ancestor of newer
            assert engine.is_ancestor(commits[1].hash, commits[0].hash)
            # Newer is not ancestor of older
            assert not engine.is_ancestor(commits[0].hash, commits[1].hash)

    def test_is_ancestor_nonexistent(self):
        engine = self._get_engine()
        assert not engine.is_ancestor("0000000000000000000000000000000000000000",
                                       engine.repo.head.commit.hexsha)

    def test_invalid_repo_path(self):
        with pytest.raises(Exception):
            GitMiningEngine("/tmp/nonexistent-repo-path-xyz")


class TestLanguageStats:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, 'test.db')

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        store.save_function(FunctionInfo("func_a", "a.py", "python", 1, 10, "function"))
        store.save_function(FunctionInfo("func_b", "b.py", "python", 1, 10, "function"))
        store.save_function(FunctionInfo("func_c", "c.js", "javascript", 1, 10, "function"))
        store.flush()
        return store

    def test_language_stats(self):
        store = self._make_store()
        stats = store.get_language_stats()
        assert len(stats) == 2
        assert stats[0]['language'] == 'python'
        assert stats[0]['count'] == 2
        assert stats[1]['language'] == 'javascript'
        assert stats[1]['count'] == 1
        store.close()

    def test_language_stats_empty(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        assert store.get_language_stats() == []
        store.close()

    def test_get_report_data(self):
        store = self._make_store()
        store.save_commit(GitCommit("aaa", "Alice", datetime(2023, 11, 14), "init", 1))
        store.save_function_change(FunctionChange(
            "func_a", "a.py", "aaa", "added", 10, 0, "Alice", datetime(2023, 11, 14), "init"))
        store.flush()
        data = store.get_report_data()
        assert 'stats' in data
        assert 'timeline' in data
        assert 'hotspots' in data
        assert 'stability' in data
        assert 'authors' in data
        assert 'languages' in data
        assert data['stats']['functions'] == 3
        assert len(data['languages']) == 2
        store.close()


class TestRenameDetection:
    """Tests for function rename/move detection."""

    def setup_method(self):
        self.tmp = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmp, ".gitast", "index.db")

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_store(self):
        store = DataStore(self.db_path)
        store.connect()
        store.create_schema()
        return store

    def _make_engine(self):
        """Create a GitMiningEngine on the gitast repo itself."""
        repo_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return GitMiningEngine(repo_path)

    def test_rename_same_file(self):
        """Renamed function in same file with same body -> detected."""
        engine = self._make_engine()
        before_src = "def old_name():\n    x = 1\n    y = 2\n    return x + y\n"
        after_src = "def new_name():\n    x = 1\n    y = 2\n    return x + y\n"
        old_func = FunctionInfo("old_name", "a.py", "python", 1, 4, "function", "def old_name()")
        new_func = FunctionInfo("new_name", "a.py", "python", 1, 4, "function", "def new_name()")

        score, signals = engine._rename_score(
            old_func, new_func,
            before_src.split('\n'), after_src.split('\n')
        )
        assert score >= RENAME_THRESHOLD
        assert signals['body'] > 0.8
        assert signals['location'] == 1.0

    def test_move_across_files(self):
        """Function deleted in one file, added in another with same body -> detected."""
        engine = self._make_engine()
        body = "def helper():\n    return 42\n"
        old_func = FunctionInfo("helper", "old_module.py", "python", 1, 2, "function", "def helper()")
        new_func = FunctionInfo("helper", "new_module.py", "python", 1, 2, "function", "def helper()")

        score, signals = engine._rename_score(
            old_func, new_func,
            body.split('\n'), body.split('\n')
        )
        # Same name, same body, different directory -> high score
        assert score >= RENAME_THRESHOLD
        assert signals['body'] == 1.0
        assert signals['name'] == 1.0

    def test_rename_with_modification(self):
        """Body changed ~30% but still detected above threshold."""
        engine = self._make_engine()
        before_src = "def process_data():\n    a = 1\n    b = 2\n    c = 3\n    d = 4\n    e = 5\n    return a + b + c + d + e\n"
        after_src = "def transform_data():\n    a = 1\n    b = 2\n    c = 3\n    d = 4\n    e = 5\n    f = 6\n    return a + b + c + d + e + f\n"
        old_func = FunctionInfo("process_data", "a.py", "python", 1, 7, "function", "def process_data()")
        new_func = FunctionInfo("transform_data", "a.py", "python", 1, 8, "function", "def transform_data()")

        score, signals = engine._rename_score(
            old_func, new_func,
            before_src.split('\n'), after_src.split('\n')
        )
        assert score >= RENAME_THRESHOLD
        assert signals['body'] > 0.6

    def test_no_false_match(self):
        """Completely different functions should not be linked."""
        engine = self._make_engine()
        before_src = "def connect_db():\n    db = sqlite3.connect('test.db')\n    return db\n"
        after_src = "def render_html():\n    template = load('index.html')\n    return template.render()\n"
        old_func = FunctionInfo("connect_db", "db.py", "python", 1, 3, "function", "def connect_db()")
        new_func = FunctionInfo("render_html", "views.py", "python", 1, 3, "function", "def render_html()")

        score, signals = engine._rename_score(
            old_func, new_func,
            before_src.split('\n'), after_src.split('\n')
        )
        assert score < RENAME_THRESHOLD

    def test_rename_score_weights(self):
        """Verify individual signal calculations."""
        engine = self._make_engine()
        src = "def foo():\n    pass\n"
        func = FunctionInfo("foo", "a.py", "python", 1, 2, "function", "def foo()")

        # Same function, same everything -> all signals ~1.0
        score, signals = engine._rename_score(func, func, src.split('\n'), src.split('\n'))
        assert signals['body'] == 1.0
        assert signals['signature'] == 1.0
        assert signals['name'] == 1.0
        assert signals['size'] == 1.0
        assert signals['location'] == 1.0
        assert score == 1.0

        # Different kind -> score 0
        func_class = FunctionInfo("foo", "a.py", "python", 1, 2, "class", "class foo")
        score2, signals2 = engine._rename_score(func, func_class, src.split('\n'), src.split('\n'))
        assert score2 == 0.0
        assert signals2 == {}

    def test_detect_renames_greedy(self):
        """_detect_renames does greedy 1:1 matching."""
        engine = self._make_engine()
        src = "def alpha():\n    return 1\ndef beta():\n    return 2\n"
        deleted = [
            FunctionInfo("old_alpha", "a.py", "python", 1, 2, "function", "def old_alpha()"),
            FunctionInfo("old_beta", "a.py", "python", 3, 4, "function", "def old_beta()"),
        ]
        added = [
            FunctionInfo("new_alpha", "a.py", "python", 1, 2, "function", "def new_alpha()"),
            FunctionInfo("new_beta", "a.py", "python", 3, 4, "function", "def new_beta()"),
        ]
        renames = engine._detect_renames(deleted, added, src.split('\n'), src.split('\n'))
        # Should get exactly 2 renames (each old matched to corresponding new by body)
        assert len(renames) == 2
        matched_pairs = {(r[0].name, r[1].name) for r in renames}
        assert ('old_alpha', 'new_alpha') in matched_pairs
        assert ('old_beta', 'new_beta') in matched_pairs

    def test_detect_function_changes_includes_renames(self):
        """detect_function_changes returns 'renamed' type for matching pairs."""
        engine = self._make_engine()
        before_src = "def old_func():\n    x = 1\n    y = 2\n    return x + y\n"
        after_src = "def new_func():\n    x = 1\n    y = 2\n    return x + y\n"
        funcs_before = [FunctionInfo("old_func", "a.py", "python", 1, 4, "function", "def old_func()")]
        funcs_after = [FunctionInfo("new_func", "a.py", "python", 1, 4, "function", "def new_func()")]

        changes = engine.detect_function_changes(funcs_before, funcs_after, before_src, after_src)
        renamed = [c for c in changes if c['change_type'] == 'renamed']
        assert len(renamed) == 1
        assert renamed[0]['old_name'] == 'old_func'
        assert renamed[0]['name'] == 'new_func'
        assert renamed[0]['confidence'] >= RENAME_THRESHOLD
        # Should NOT have separate added/deleted entries
        added = [c for c in changes if c['change_type'] == 'added']
        deleted = [c for c in changes if c['change_type'] == 'deleted']
        assert len(added) == 0
        assert len(deleted) == 0

    def test_history_follows_renames(self):
        """Save renames, query history, verify chain following."""
        store = self._make_store()
        # Save commits
        store.save_commit(GitCommit("aaa", "Alice", datetime(2023, 1, 1), "add func", 1))
        store.save_commit(GitCommit("bbb", "Alice", datetime(2023, 2, 1), "rename func", 1))
        store.save_commit(GitCommit("ccc", "Alice", datetime(2023, 3, 1), "modify func", 1))

        # old_func in a.py -> renamed to new_func in a.py
        store.save_function_change(FunctionChange(
            "old_func", "a.py", "aaa", "added", 10, 0, "Alice", datetime(2023, 1, 1), "add func"))
        store.save_function_change(FunctionChange(
            "new_func", "a.py", "bbb", "renamed", 0, 0, "Alice", datetime(2023, 2, 1), "rename func",
            renamed_from="old_func"))
        store.save_function_change(FunctionChange(
            "new_func", "a.py", "ccc", "modified", 5, 2, "Alice", datetime(2023, 3, 1), "modify func"))

        # Record the rename link
        store.save_function_rename("bbb", "old_func", "a.py", "function",
                                    "new_func", "a.py", "function", 0.95)
        store.flush()

        # Query by new name should include old history
        history = store.get_function_history("a.py", "new_func")
        assert len(history) == 3
        names = {h.function_name for h in history}
        assert 'old_func' in names
        assert 'new_func' in names

        # Query by old name should also find chain
        history_old = store.get_function_history("a.py", "old_func")
        assert len(history_old) == 3

        store.close()

    def test_history_by_name_follows_renames(self):
        """get_function_history_by_name follows rename chains."""
        store = self._make_store()
        store.save_commit(GitCommit("aaa", "Bob", datetime(2023, 1, 1), "init", 1))
        store.save_commit(GitCommit("bbb", "Bob", datetime(2023, 2, 1), "move", 1))

        store.save_function_change(FunctionChange(
            "helper", "old.py", "aaa", "added", 5, 0, "Bob", datetime(2023, 1, 1), "init"))
        store.save_function_change(FunctionChange(
            "helper", "new.py", "bbb", "renamed", 0, 0, "Bob", datetime(2023, 2, 1), "move",
            renamed_from="helper"))
        store.save_function_rename("bbb", "helper", "old.py", "function",
                                    "helper", "new.py", "function", 0.90)
        store.flush()

        history = store.get_function_history_by_name("helper")
        assert len(history) == 2
        file_paths = {h.file_path for h in history}
        assert 'old.py' in file_paths
        assert 'new.py' in file_paths

        store.close()

    def test_schema_includes_function_renames(self):
        """function_renames table exists after schema creation."""
        store = self._make_store()
        tables = store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t['name'] for t in tables]
        assert 'function_renames' in table_names
        store.close()

    def test_clear_all_clears_renames(self):
        """clear_all removes function_renames data."""
        store = self._make_store()
        store.save_commit(GitCommit("aaa", "Alice", datetime(2023, 1, 1), "test", 1))
        store.save_function_rename("aaa", "old", "a.py", "function",
                                    "new", "a.py", "function", 0.9)
        store.flush()
        count = store.conn.execute("SELECT COUNT(*) as c FROM function_renames").fetchone()['c']
        assert count == 1
        store.clear_all()
        count = store.conn.execute("SELECT COUNT(*) as c FROM function_renames").fetchone()['c']
        assert count == 0
        store.close()
