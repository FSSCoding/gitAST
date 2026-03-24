"""Tests for GitAST data models"""
from datetime import datetime

from gitast.models import GitCommit, FunctionInfo, BlameEntry, FunctionChange


def test_git_commit_creation():
    c = GitCommit(
        hash="abc123def456",
        author="Test Author",
        timestamp=datetime(2025, 1, 15, 10, 30),
        message="Fix authentication bug",
        files_changed=3,
    )
    assert c.hash == "abc123def456"
    assert c.author == "Test Author"
    assert c.files_changed == 3
    assert c.semantic_tags == []


def test_git_commit_semantic_tags_default_empty():
    c = GitCommit(hash="abc", author="X", timestamp=datetime(2025, 1, 1),
                  message="msg", files_changed=1)
    assert c.semantic_tags == []
    assert isinstance(c.semantic_tags, list)


def test_function_change_from_commit_context():
    """Mirrors how cli.py constructs FunctionChange during indexing."""
    commit = GitCommit(
        hash="abc123def456", author="Alice",
        timestamp=datetime(2025, 6, 1, 12, 0),
        message="Refactor auth module to improve token handling",
        files_changed=3,
    )
    change = FunctionChange(
        function_name="refresh_token",
        file_path="src/auth.py",
        commit_hash=commit.hash,
        change_type="modified",
        lines_added=10 // 2,
        lines_removed=4 // 2,
        author=commit.author,
        timestamp=commit.timestamp,
        message=commit.message[:200],
    )
    assert change.author == "Alice"
    assert change.commit_hash == "abc123def456"
    assert change.timestamp == datetime(2025, 6, 1, 12, 0)
    assert change.message == "Refactor auth module to improve token handling"
    assert change.lines_added == 5
    assert change.lines_removed == 2


def test_function_info():
    f = FunctionInfo(
        name="process_data",
        file_path="src/main.py",
        language="python",
        start_line=10,
        end_line=25,
        kind="function",
        signature="def process_data(input: str) -> dict:",
    )
    assert f.name == "process_data"
    assert f.line_count == 16
    assert f.kind == "function"


def test_function_info_line_count():
    f = FunctionInfo(
        name="x", file_path="a.py", language="python",
        start_line=1, end_line=1, kind="function",
    )
    assert f.line_count == 1


def test_git_commit_repr_short_message():
    c = GitCommit(hash="abc", author="X", timestamp=datetime(2025, 1, 1),
                  message="Short", files_changed=1)
    r = repr(c)
    assert "Short" in r
    assert "..." not in r


def test_git_commit_repr_long_message():
    c = GitCommit(hash="abc", author="X", timestamp=datetime(2025, 1, 1),
                  message="A" * 50, files_changed=1)
    r = repr(c)
    assert "..." in r


def test_blame_entry():
    e = BlameEntry(
        function_name="login",
        file_path="auth.py",
        author="Alice",
        line_count=20,
        percentage=66.7,
        commit_hash="abc123",
    )
    assert e.percentage == 66.7
    assert e.author == "Alice"


def test_function_change():
    ch = FunctionChange(
        function_name="save",
        file_path="db.py",
        commit_hash="def456",
        change_type="modified",
        lines_added=5,
        lines_removed=2,
        author="Bob",
        timestamp=datetime(2025, 3, 1),
        message="Optimize save query",
    )
    assert ch.change_type == "modified"
    assert ch.lines_added == 5


def test_function_change_defaults():
    ch = FunctionChange(
        function_name="f", file_path="x.py",
        commit_hash="aaa", change_type="added",
    )
    assert ch.lines_added == 0
    assert ch.lines_removed == 0
    assert ch.author == ""
    assert ch.timestamp is None


def test_function_info_invalid_line_range_corrected():
    """end_line < start_line should be corrected to start_line."""
    f = FunctionInfo(
        name="bad", file_path="x.py", language="python",
        start_line=100, end_line=10, kind="function",
    )
    assert f.end_line == f.start_line
    assert f.line_count == 1


def test_function_info_negative_start_line_corrected():
    """Negative start_line should be corrected to 1."""
    f = FunctionInfo(
        name="bad", file_path="x.py", language="python",
        start_line=-5, end_line=10, kind="function",
    )
    assert f.start_line == 1
    assert f.end_line == 10


def test_function_info_zero_start_line_corrected():
    """Zero start_line should be corrected to 1."""
    f = FunctionInfo(
        name="bad", file_path="x.py", language="python",
        start_line=0, end_line=5, kind="function",
    )
    assert f.start_line == 1
