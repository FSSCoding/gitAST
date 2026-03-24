"""Data models for GitAST"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class GitCommit:
    """A git commit with metadata."""
    hash: str
    author: str
    timestamp: datetime
    message: str
    files_changed: int
    semantic_tags: List[str] = field(default_factory=list)

    def __repr__(self):
        msg = self.message[:30]
        ellipsis = '...' if len(self.message) > 30 else ''
        return f"GitCommit(hash='{self.hash[:8]}', author='{self.author}', message='{msg}{ellipsis}')"


@dataclass
class FunctionInfo:
    """A function or class definition extracted via AST."""
    name: str
    file_path: str
    language: str
    start_line: int
    end_line: int
    kind: str  # 'function', 'method', 'class'
    signature: str = ""
    docstring: str = ""

    def __post_init__(self):
        if self.start_line < 1:
            self.start_line = 1
        if self.end_line < self.start_line:
            self.end_line = self.start_line

    @property
    def line_count(self) -> int:
        return self.end_line - self.start_line + 1


@dataclass
class BlameEntry:
    """Blame data for a function."""
    function_name: str
    file_path: str
    author: str
    line_count: int
    percentage: float
    commit_hash: str = ""


@dataclass
class FunctionChange:
    """A change to a function in a specific commit."""
    function_name: str
    file_path: str
    commit_hash: str
    change_type: str  # 'added', 'modified', 'deleted', 'renamed'
    lines_added: int = 0
    lines_removed: int = 0
    author: str = ""
    timestamp: Optional[datetime] = None
    message: str = ""
    renamed_from: Optional[str] = None


@dataclass
class ConfigChange:
    """A change to a key-path in a structured config file."""
    file_path: str
    key_path: str
    commit_hash: str
    change_type: str  # 'added', 'modified', 'deleted'
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    author: str = ""
    timestamp: Optional[datetime] = None
    message: str = ""


@dataclass
class DepChange:
    """A change to a dependency in a package/dependency file."""
    file_path: str
    package: str
    commit_hash: str
    change_type: str  # 'added', 'removed', 'bumped'
    old_version: Optional[str] = None
    new_version: Optional[str] = None
    author: str = ""
    timestamp: Optional[datetime] = None
    message: str = ""
