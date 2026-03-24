"""Git hook management for GitAST auto-indexing."""
import os
import shutil
import stat
from typing import Dict, List

HOOK_MARKER_START = '# --- GitAST auto-index ---'
HOOK_MARKER_END = '# --- /GitAST auto-index ---'


def _get_hook_snippet(gitast_path: str) -> str:
    """Build hook script snippet using absolute path to gitast."""
    return (
        f'{HOOK_MARKER_START}\n'
        f'{gitast_path} index . 2>&1 | tail -1 &\n'
        f'{HOOK_MARKER_END}\n'
    )


def _hooks_dir(repo_path: str) -> str:
    """Return the .git/hooks directory, raising if not a git repo.

    Handles .git as a file (worktrees, submodules) by following the gitdir pointer.
    """
    git_path = os.path.join(repo_path, '.git')
    if os.path.isfile(git_path):
        # Worktree or submodule: .git is a file with "gitdir: <path>"
        with open(git_path, 'r') as f:
            content = f.read().strip()
        if content.startswith('gitdir:'):
            git_dir = content.split(':', 1)[1].strip()
            if not os.path.isabs(git_dir):
                git_dir = os.path.join(repo_path, git_dir)
            git_dir = os.path.normpath(git_dir)
        else:
            raise FileNotFoundError(f"Not a git repository: {repo_path}")
    elif os.path.isdir(git_path):
        git_dir = git_path
    else:
        raise FileNotFoundError(f"Not a git repository: {repo_path}")
    hooks_dir = os.path.join(git_dir, 'hooks')
    os.makedirs(hooks_dir, exist_ok=True)
    return hooks_dir


def install_hooks(repo_path: str) -> List[str]:
    """Install post-commit and post-merge hooks. Returns list of installed hook names.

    Appends to existing hooks. Skips if already installed (marker present).
    Uses absolute path to gitast binary for reliable execution in hook environment.
    """
    hooks_dir = _hooks_dir(repo_path)
    installed = []

    # Resolve absolute path to gitast for hook reliability
    gitast_path = shutil.which('gitast') or 'gitast'
    snippet = _get_hook_snippet(gitast_path)

    for hook_name in ('post-commit', 'post-merge'):
        hook_path = os.path.join(hooks_dir, hook_name)

        # Read existing content
        existing = ''
        if os.path.exists(hook_path):
            with open(hook_path, 'r') as f:
                existing = f.read()

        # Skip if already installed
        if HOOK_MARKER_START in existing:
            continue

        # Append (add shebang if new file)
        with open(hook_path, 'a') as f:
            if not existing:
                f.write('#!/bin/sh\n')
            elif not existing.endswith('\n'):
                f.write('\n')
            f.write(snippet)

        # Ensure executable
        st = os.stat(hook_path)
        os.chmod(hook_path, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        installed.append(hook_name)

    return installed


def uninstall_hooks(repo_path: str) -> List[str]:
    """Remove GitAST hook sections. Returns list of removed hook names."""
    hooks_dir = _hooks_dir(repo_path)
    removed = []

    for hook_name in ('post-commit', 'post-merge'):
        hook_path = os.path.join(hooks_dir, hook_name)

        if not os.path.exists(hook_path):
            continue

        with open(hook_path, 'r') as f:
            content = f.read()

        if HOOK_MARKER_START not in content:
            continue

        # Remove the marked section
        lines = content.split('\n')
        new_lines = []
        inside_marker = False
        for line in lines:
            if line.strip() == HOOK_MARKER_START:
                inside_marker = True
                continue
            if line.strip() == HOOK_MARKER_END:
                inside_marker = False
                continue
            if not inside_marker:
                new_lines.append(line)

        new_content = '\n'.join(new_lines).rstrip('\n') + '\n'

        # If only shebang remains, remove the file entirely
        if new_content.strip() in ('', '#!/bin/sh'):
            os.remove(hook_path)
        else:
            with open(hook_path, 'w') as f:
                f.write(new_content)

        removed.append(hook_name)

    return removed


def get_hook_status(repo_path: str) -> Dict[str, bool]:
    """Check which hooks are installed."""
    try:
        hooks_dir = _hooks_dir(repo_path)
    except FileNotFoundError:
        return {name: False for name in ('post-commit', 'post-merge')}

    status = {}
    for hook_name in ('post-commit', 'post-merge'):
        hook_path = os.path.join(hooks_dir, hook_name)
        if os.path.exists(hook_path):
            with open(hook_path, 'r') as f:
                status[hook_name] = HOOK_MARKER_START in f.read()
        else:
            status[hook_name] = False
    return status
