"""Tests for GitAST display utilities"""
from datetime import datetime
from io import StringIO

from rich.console import Console

from gitast.utils import (
    display_search_results, display_function_history,
    display_blame, display_index_stats,
    display_hotspots, display_blame_summary,
    display_find_results, display_ages, display_timeline,
    display_commit_diff, display_file_report, display_status,
    display_stability, _clean_name, _abbrev_path,
)
from gitast.models import FunctionChange, BlameEntry


def _capture_output(func, *args, **kwargs):
    """Capture Rich console output from a display function (plain text, no ANSI)."""
    import gitast.utils as utils_mod
    old_console = utils_mod.console
    buf = StringIO()
    utils_mod.console = Console(file=buf, no_color=True, width=120)
    try:
        func(*args, **kwargs)
        return buf.getvalue()
    finally:
        utils_mod.console = old_console


class TestDisplayFunctions:
    def test_search_results_empty(self):
        output = _capture_output(display_search_results, [], "test query")
        assert "No results found" in output

    def test_search_results_with_data(self):
        results = [
            {'type': 'commit', 'name': 'abc123', 'file_path': '',
             'author': 'Alice', 'content': 'Fix auth bug', 'detail': 'commit abc123',
             'score': 1.5},
            {'type': 'function', 'name': 'login', 'file_path': 'auth.py',
             'author': 'Bob', 'content': 'def login', 'detail': 'function in auth.py',
             'score': 1.0},
        ]
        output = _capture_output(display_search_results, results, "auth")
        assert "auth" in output
        assert "2 results" in output

    def test_function_history_empty(self):
        output = _capture_output(display_function_history, [], "app.py", "process")
        assert "No history found" in output

    def test_function_history_with_data(self):
        changes = [
            FunctionChange(
                function_name="process", file_path="app.py",
                commit_hash="abc12345", change_type="added",
                lines_added=10, lines_removed=0,
                author="Alice", timestamp=datetime(2025, 6, 1),
                message="Add process function",
            ),
        ]
        output = _capture_output(display_function_history, changes, "app.py", "process")
        assert "process" in output
        assert "1 changes" in output or "1 change" in output

    def test_blame_empty(self):
        output = _capture_output(display_blame, [], "app.py", "process")
        assert "No blame data" in output

    def test_blame_with_data(self):
        entries = [
            BlameEntry(function_name="process", file_path="app.py",
                       author="Alice", line_count=15, percentage=75.0,
                       commit_hash="abc123"),
            BlameEntry(function_name="process", file_path="app.py",
                       author="Bob", line_count=5, percentage=25.0,
                       commit_hash="def456"),
        ]
        output = _capture_output(display_blame, entries, "app.py", "process")
        assert "Alice" in output
        assert "75.0%" in output

    def test_index_stats(self):
        stats = {"commits": 10, "functions": 50, "changes": 100, "blame_entries": 30}
        output = _capture_output(display_index_stats, stats)
        assert "10" in output
        assert "50" in output


class TestCleanName:
    def test_plain_name(self):
        assert _clean_name("my_func") == "my_func"

    def test_newline_collapsed(self):
        result = _clean_name("ONAL CODE ONLY\n     Initia")
        assert "\n" not in result
        assert "ONAL CODE ONLY" in result

    def test_truncation(self):
        long_name = "a" * 50
        result = _clean_name(long_name, max_len=40)
        assert len(result) <= 40
        assert result.endswith("…")

    def test_tabs_and_spaces(self):
        result = _clean_name("foo\t  bar")
        assert result == "foo bar"

    def test_exact_max_len_not_truncated(self):
        name = "a" * 40
        result = _clean_name(name, max_len=40)
        assert "…" not in result


class TestAbbrevPath:
    def test_short_path_unchanged(self):
        assert _abbrev_path("src/foo.py") == "src/foo.py"

    def test_long_path_abbreviated(self):
        long = "src/collection_system/unified_collection_api.py"
        result = _abbrev_path(long, max_len=40)
        assert len(result) <= 40
        assert result.startswith("…")

    def test_long_path_keeps_filename(self):
        long = "src/a/b/myfile.py"
        result = _abbrev_path(long, max_len=40)
        # Short enough to keep last 2 parts
        assert "myfile.py" in result

    def test_exactly_at_limit_unchanged(self):
        path = "a" * 40
        assert _abbrev_path(path, max_len=40) == path


class TestHotspotDisplay:
    def test_hotspots_empty(self):
        output = _capture_output(display_hotspots, [])
        assert "No function changes" in output

    def test_hotspots_renders(self):
        results = [
            {
                'function_name': 'my_func',
                'file_path': 'src/core.py',
                'change_count': 15,
                'author_count': 2,
                'added': 3,
                'modified': 10,
                'deleted': 2,
                'last_changed': datetime(2025, 8, 10),
            }
        ]
        output = _capture_output(display_hotspots, results)
        assert "my_func" in output
        assert "core.py" in output

    def test_hotspots_dirty_name_rendered_clean(self):
        """Garbled names with newlines should render on a single clean line."""
        results = [
            {
                'function_name': 'ifiedSearchOrchestrator:\n\n    se',
                'file_path': 'src/search/unified_search_orchestrator.py',
                'change_count': 30,
                'author_count': 1,
                'added': 1,
                'modified': 29,
                'deleted': 0,
                'last_changed': datetime(2025, 8, 18),
            }
        ]
        output = _capture_output(display_hotspots, results)
        # Should render without crashing and name should not span multiple blank rows
        assert "30" in output


class TestBlameSummaryDisplay:
    def test_blame_summary_empty(self):
        output = _capture_output(display_blame_summary, [], "foo.py")
        assert "No functions indexed" in output

    def test_blame_summary_renders(self):
        results = [
            {
                'name': 'my_method',
                'kind': 'method',
                'start_line': 10,
                'end_line': 30,
                'language': 'python',
                'primary_owner': 'Alice',
                'ownership_pct': 100.0,
                'change_count': 5,
            }
        ]
        output = _capture_output(display_blame_summary, results, "app.py")
        assert "my_method" in output
        assert "Alice" in output
        assert "100.0%" in output


class TestFindDisplay:
    def test_find_empty(self):
        output = _capture_output(display_find_results, [], "test")
        assert "No functions matching" in output

    def test_find_renders(self):
        results = [
            {'name': 'search_func', 'file_path': 'engine.py', 'language': 'python',
             'start_line': 10, 'end_line': 25, 'kind': 'function',
             'signature': 'def search_func(query):'},
        ]
        output = _capture_output(display_find_results, results, "search")
        assert "search_func" in output
        assert "engine.py" in output
        assert "1 functions" in output


class TestAgeDisplay:
    def test_age_empty(self):
        output = _capture_output(display_ages, [])
        assert "No function data" in output

    def test_age_renders(self):
        results = [
            {'name': 'old_func', 'file_path': 'core.py', 'kind': 'function',
             'last_changed': datetime(2024, 1, 1), 'days_ago': 400,
             'change_count': 3},
        ]
        output = _capture_output(display_ages, results)
        assert "old_func" in output
        assert "400d" in output

    def test_age_recent_label(self):
        output = _capture_output(display_ages, [], recent=True)
        assert "newest" in output.lower() or "No function data" in output


class TestTimelineDisplay:
    def test_timeline_empty(self):
        output = _capture_output(display_timeline, [])
        assert "No activity data" in output

    def test_timeline_renders(self):
        results = [
            {'month': '2025-01', 'commits': 5, 'changes': 20,
             'functions': 8, 'authors': 2},
            {'month': '2025-02', 'commits': 3, 'changes': 10,
             'functions': 5, 'authors': 1},
        ]
        output = _capture_output(display_timeline, results)
        assert "2025-01" in output
        assert "2 months" in output


class TestCommitDiffDisplay:
    def test_diff_empty(self):
        output = _capture_output(display_commit_diff, [], "abc123")
        assert "No function changes" in output

    def test_diff_renders(self):
        results = [
            {'function_name': 'my_func', 'file_path': 'app.py',
             'change_type': 'modified', 'lines_added': 5,
             'lines_removed': 2, 'author': 'Alice', 'commit_hash': 'abc123'},
        ]
        output = _capture_output(display_commit_diff, results, "abc123")
        assert "my_func" in output
        assert "1 changes" in output


class TestFileReportDisplay:
    def test_file_report_renders(self):
        report = {
            'file_path': 'src/core.py',
            'language': 'python',
            'total_functions': 2,
            'total_changes': 5,
            'unique_owners': 1,
            'functions': [
                {'name': 'func_a', 'kind': 'function', 'start_line': 1,
                 'end_line': 10, 'owner': 'Alice', 'ownership_pct': 100.0,
                 'change_count': 3, 'last_changed': datetime(2025, 6, 1),
                 'days_ago': 200},
                {'name': 'func_b', 'kind': 'method', 'start_line': 12,
                 'end_line': 20, 'owner': '', 'ownership_pct': 0.0,
                 'change_count': 0, 'last_changed': None, 'days_ago': -1},
            ],
        }
        output = _capture_output(display_file_report, report)
        assert "src/core.py" in output
        assert "func_a" in output
        assert "func_b" in output
        assert "2 functions" in output


class TestStabilityDisplay:
    def test_stability_empty(self):
        output = _capture_output(display_stability, [])
        assert "No function data" in output

    def test_stability_renders(self):
        results = [
            {'function_name': 'stable_func', 'file_path': 'core.py',
             'stability_score': 0.95, 'change_count': 1, 'author_count': 1,
             'days_ago': 200, 'rating': 'stable'},
            {'function_name': 'volatile_func', 'file_path': 'api.py',
             'stability_score': 0.15, 'change_count': 30, 'author_count': 5,
             'days_ago': 2, 'rating': 'critical'},
        ]
        output = _capture_output(display_stability, results)
        assert "stable_func" in output
        assert "volatile_func" in output
        assert "Most Stable" in output

    def test_stability_volatile_ordering(self):
        results = [
            {'function_name': 'stable_func', 'file_path': 'core.py',
             'stability_score': 0.95, 'change_count': 1, 'author_count': 1,
             'days_ago': 200, 'rating': 'stable'},
        ]
        output = _capture_output(display_stability, results, volatile=True)
        assert "Most Volatile" in output
