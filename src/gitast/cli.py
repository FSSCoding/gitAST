"""CLI interface for GitAST"""
import csv
import hashlib
import json
import os
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import git
import click
from rich.console import Console

from . import __version__
from .core import DataStore, GitMiningEngine, parse_date_filter
from .analysis import ASTParser, BlameAnalyzer
from .models import GitCommit, FunctionChange
from .utils import (
    display_search_results, display_function_history,
    display_blame, display_index_stats,
    display_hotspots, display_blame_summary,
    display_authors, display_commits, display_show,
    display_find_results, display_ages, display_timeline,
    display_commit_diff, display_file_report, display_status,
    display_stability, console,
)
from .embed import (
    EmbeddingClient, prepare_function_text, prepare_commit_text,
    classify_query,
)

DEFAULT_DB = ".gitast/index.db"
CLONE_DIR = "/tmp/gitast-clones"


def _is_remote_url(path: str) -> bool:
    """Check if path looks like a remote git URL."""
    return bool(re.match(r'^(https?://|git@|ssh://)', path))


def _resolve_remote(url: str) -> str:
    """Clone a remote URL to /tmp/gitast-clones/<name> and return local path.

    Reuses existing clones if they exist (fetches latest).
    """
    # Extract repo name from URL
    name = re.sub(r'\.git$', '', url.rstrip('/')).rsplit('/', 1)[-1]
    # Use hash suffix to avoid collisions between repos with same name
    url_hash = hashlib.md5(url.encode(), usedforsecurity=False).hexdigest()[:8]
    clone_path = os.path.join(CLONE_DIR, f"{name}-{url_hash}")

    if os.path.exists(os.path.join(clone_path, '.git')):
        console.print(f"[dim]Updating existing clone: {clone_path}[/dim]")
        try:
            repo = git.Repo(clone_path)
            repo.remotes.origin.fetch()
            # Reset to latest default branch
            default_branch = repo.active_branch.name
            repo.git.reset('--hard', f'origin/{default_branch}')
        except Exception as e:
            console.print(f"[yellow]Fetch failed ({e}), re-cloning...[/yellow]")
            shutil.rmtree(clone_path)
            console.print(f"[cyan]Cloning:[/cyan] {url}")
            git.Repo.clone_from(url, clone_path)
    else:
        os.makedirs(CLONE_DIR, exist_ok=True)
        console.print(f"[cyan]Cloning:[/cyan] {url}")
        git.Repo.clone_from(url, clone_path)

    console.print(f"[dim]Local path: {clone_path}[/dim]")
    return clone_path


def _resolve_path(path: str) -> str:
    """Expand ~ and resolve to absolute path. Clones remote URLs to /tmp."""
    if _is_remote_url(path):
        return _resolve_remote(path)
    return os.path.abspath(os.path.expanduser(path))


def _get_engine(path: str) -> GitMiningEngine:
    try:
        return GitMiningEngine(path)
    except (git.InvalidGitRepositoryError, git.NoSuchPathError):
        console.print(f"[red]Not a git repository: {path}[/red]")
        raise SystemExit(1)


def _resolve_tag_or_ref(path: str, ref: str) -> str:
    """Resolve a tag name or git ref to a commit hash. Returns ref unchanged if not resolvable."""
    try:
        engine = _get_engine(path)
        commit = engine.repo.commit(ref)
        return commit.hexsha
    except Exception:
        return ref


@click.group()
@click.version_option(version=__version__, prog_name="gitast")
def main():
    """GitAST - Semantic git history search with function-level tracking."""
    pass


def _run_phase4(engine, store, parser, commits, max_commits):
    """Phase 4: Track function changes across commits. Shared by full and incremental index."""
    from .core import RENAME_THRESHOLD
    change_count = 0
    change_limit = max_commits if max_commits else len(commits)
    change_limit = min(change_limit, len(commits))
    if change_limit < len(commits):
        console.print(f"  [dim]Analyzing {change_limit} of {len(commits)} commits[/dim]")

    for i in range(change_limit):
        commit = commits[i]
        changed_files = engine.get_changed_files(commit.hash)
        parent_hash = engine.get_parent_hash(commit.hash)

        # Collect per-file results first, then do cross-file rename detection
        all_added = []
        all_deleted = []
        per_file_changes = []  # (file_path, file_added, file_removed, detected_list)
        per_file_sources = {}  # file_path -> (before_lines, after_lines)

        for cf in changed_files:
            file_path = cf['path']
            ext = os.path.splitext(file_path)[1]
            language = engine.SUPPORTED_EXTENSIONS.get(ext)
            if not language:
                continue

            source_after = engine.get_file_at_commit(commit.hash, file_path) or ""
            funcs_after = parser.parse_file(source_after, file_path, language) if source_after else []

            source_before = ""
            funcs_before = []
            if parent_hash:
                source_before = engine.get_file_at_commit(parent_hash, file_path) or ""
                if source_before:
                    funcs_before = parser.parse_file(source_before, file_path, language)

            detected = engine.detect_function_changes(
                funcs_before, funcs_after, source_before, source_after
            )
            file_added = cf.get('insertions', 0)
            file_removed = cf.get('deletions', 0)

            per_file_sources[file_path] = (
                source_before.split('\n') if source_before else [],
                source_after.split('\n') if source_after else []
            )

            # Separate per-file renames (already detected within same file) from adds/deletes
            file_renames = [d for d in detected if d['change_type'] == 'renamed']
            file_other = [d for d in detected if d['change_type'] != 'renamed']

            # Collect cross-file candidates
            for d in file_other:
                if d['change_type'] == 'added':
                    all_added.append((file_path, d['func'], file_added, file_removed))
                elif d['change_type'] == 'deleted':
                    all_deleted.append((file_path, d['func'], file_added, file_removed))

            per_file_changes.append((file_path, file_added, file_removed, file_other, file_renames))

        # Cross-file rename detection on remaining added/deleted
        cross_renames = []
        if all_added and all_deleted:
            # Build combined before/after lines keyed by file
            del_funcs = [f for _, f, _, _ in all_deleted]
            add_funcs = [f for _, f, _, _ in all_added]
            # We need before_lines for deleted funcs and after_lines for added funcs
            # _rename_score uses func.file_path to index, so we need combined lines
            # Build a merged line set per file_path
            all_before = {}
            all_after = {}
            for fp, (bl, al) in per_file_sources.items():
                all_before[fp] = bl
                all_after[fp] = al

            candidates = []
            for old_func in del_funcs:
                old_bl = all_before.get(old_func.file_path, [])
                for new_func in add_funcs:
                    new_al = all_after.get(new_func.file_path, [])
                    if old_bl and new_al:
                        score, signals = engine._rename_score(old_func, new_func, old_bl, new_al)
                        if score >= RENAME_THRESHOLD:
                            candidates.append((old_func, new_func, score, signals))

            candidates.sort(key=lambda x: x[2], reverse=True)
            used_old = set()
            used_new = set()
            for old_func, new_func, score, signals in candidates:
                old_key = (old_func.name, old_func.file_path, old_func.kind)
                new_key = (new_func.name, new_func.file_path, new_func.kind)
                if old_key not in used_old and new_key not in used_new:
                    used_old.add(old_key)
                    used_new.add(new_key)
                    cross_renames.append((old_func, new_func, score, signals))

        cross_renamed_old = {(r[0].name, r[0].file_path, r[0].kind) for r in cross_renames}
        cross_renamed_new = {(r[1].name, r[1].file_path, r[1].kind) for r in cross_renames}

        # Save all changes
        for file_path, file_added, file_removed, file_other, file_renames in per_file_changes:
            all_detected = file_other + file_renames
            n_changed = len(all_detected) or 1

            for d in file_renames:
                # Intra-file renames already detected
                func = d['func']
                change = FunctionChange(
                    function_name=func.name,
                    file_path=file_path,
                    commit_hash=commit.hash,
                    change_type='renamed',
                    lines_added=file_added // n_changed,
                    lines_removed=file_removed // n_changed,
                    author=commit.author,
                    timestamp=commit.timestamp,
                    message=commit.message[:200],
                    renamed_from=d.get('old_name'),
                )
                store.save_function_change(change)
                import json as _json
                store.save_function_rename(
                    commit.hash, d['old_name'], d['old_file_path'], d['old_kind'],
                    func.name, file_path, func.kind, d['confidence'],
                    _json.dumps(d.get('signals', {}))
                )
                change_count += 1

            for d in file_other:
                func = d['func']
                ct = d['change_type']
                fkey = (func.name, file_path, func.kind)
                # Skip if matched as cross-file rename
                if ct == 'added' and fkey in cross_renamed_new:
                    continue
                if ct == 'deleted' and fkey in cross_renamed_old:
                    continue
                change = FunctionChange(
                    function_name=func.name,
                    file_path=file_path,
                    commit_hash=commit.hash,
                    change_type=ct,
                    lines_added=file_added // n_changed if ct != 'deleted' else 0,
                    lines_removed=file_removed // n_changed if ct != 'added' else 0,
                    author=commit.author,
                    timestamp=commit.timestamp,
                    message=commit.message[:200],
                )
                store.save_function_change(change)
                change_count += 1

        # Save cross-file renames
        for old_func, new_func, score, signals in cross_renames:
            change = FunctionChange(
                function_name=new_func.name,
                file_path=new_func.file_path,
                commit_hash=commit.hash,
                change_type='renamed',
                lines_added=0,
                lines_removed=0,
                author=commit.author,
                timestamp=commit.timestamp,
                message=commit.message[:200],
                renamed_from=old_func.name,
            )
            store.save_function_change(change)
            import json as _json
            store.save_function_rename(
                commit.hash, old_func.name, old_func.file_path, old_func.kind,
                new_func.name, new_func.file_path, new_func.kind, score,
                _json.dumps(signals)
            )
            change_count += 1

    store.flush()
    return change_count


def _run_config_phase(engine, store, commits, max_commits):
    """Track config key-path changes across commits."""
    from .config import is_config_file, parse_config, diff_configs
    from .models import ConfigChange

    change_count = 0
    limit = min(max_commits or len(commits), len(commits))

    for i in range(limit):
        commit = commits[i]
        changed_files = engine.get_changed_files(commit.hash)
        parent_hash = engine.get_parent_hash(commit.hash)

        for cf in changed_files:
            file_path = cf['path']
            if not is_config_file(file_path):
                continue

            after_content = engine.get_file_at_commit(commit.hash, file_path) or ""
            before_content = ""
            if parent_hash:
                before_content = engine.get_file_at_commit(parent_hash, file_path) or ""

            before_dict = parse_config(before_content, file_path)
            after_dict = parse_config(after_content, file_path)

            if before_dict is None and after_dict is None:
                continue

            diffs = diff_configs(before_dict, after_dict)
            for key_path, change_type, old_val, new_val in diffs:
                store.save_config_change(ConfigChange(
                    file_path=file_path,
                    key_path=key_path,
                    commit_hash=commit.hash,
                    change_type=change_type,
                    old_value=old_val,
                    new_value=new_val,
                    author=commit.author,
                    timestamp=commit.timestamp,
                    message=commit.message,
                ))
                change_count += 1

    store.flush()
    return change_count


def _run_dep_phase(engine, store, commits, max_commits):
    """Track dependency changes across commits."""
    from .deps import is_dep_file, parse_deps, diff_deps
    from .models import DepChange

    change_count = 0
    limit = min(max_commits or len(commits), len(commits))

    for i in range(limit):
        commit = commits[i]
        changed_files = engine.get_changed_files(commit.hash)
        parent_hash = engine.get_parent_hash(commit.hash)

        for cf in changed_files:
            file_path = cf['path']
            if not is_dep_file(file_path):
                continue

            after_content = engine.get_file_at_commit(commit.hash, file_path) or ""
            before_content = ""
            if parent_hash:
                before_content = engine.get_file_at_commit(parent_hash, file_path) or ""

            before_deps = parse_deps(before_content, file_path) if before_content else {}
            after_deps = parse_deps(after_content, file_path) if after_content else {}

            diffs = diff_deps(before_deps, after_deps)
            for pkg, change_type, old_ver, new_ver in diffs:
                store.save_dep_change(DepChange(
                    file_path=file_path,
                    package=pkg,
                    commit_hash=commit.hash,
                    change_type=change_type,
                    old_version=old_ver,
                    new_version=new_ver,
                    author=commit.author,
                    timestamp=commit.timestamp,
                    message=commit.message,
                ))
                change_count += 1

    store.flush()
    return change_count


def _run_embedding_phase(store, embed_model: str, embed_endpoint: str = None,
                         force: bool = False) -> None:
    """Phase 6: Build semantic embeddings. Gracefully handles failures."""
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
    import numpy as np

    console.print("\n[cyan]Phase 6:[/cyan] Building semantic embeddings...")

    # Set up embedding client
    from .llm import LLMConfig
    config = LLMConfig()
    if embed_endpoint:
        config.endpoint = embed_endpoint
    client = EmbeddingClient(config=config, model=embed_model)

    console.print("  Checking embedding endpoint...", end=" ")
    endpoint = client.health_check()
    if not endpoint:
        console.print("[yellow]unavailable — skipping embeddings[/yellow]")
        return
    # After health_check, client.model is resolved (auto-detected or confirmed)
    embed_model = client.model
    console.print(f"[green]ok: {endpoint} ({embed_model})[/green]")

    # Check model change
    stored_model = store.get_meta('embed_model')
    if stored_model and stored_model != embed_model:
        console.print(f"  [yellow]Model changed ({stored_model} -> {embed_model}). Re-embedding all.[/yellow]")
        store.clear_embeddings()
        force = True

    if force:
        store.clear_embeddings()

    # Gather functions to embed
    existing_func_ids = store.get_embedded_ref_ids('function')
    existing_commit_ids = store.get_embedded_ref_ids('commit')

    func_rows = store.conn.execute("SELECT name, file_path, kind, signature, docstring FROM functions").fetchall()
    func_texts = []
    for r in func_rows:
        ref_id = f"{r['name']}::{r['file_path']}"
        if ref_id not in existing_func_ids:
            text = prepare_function_text(
                r['name'], r['kind'], r['file_path'], r['signature'],
                r['docstring'] or '', store._split_identifiers
            )
            func_texts.append(('function', ref_id, text))

    commit_rows = store.conn.execute("SELECT hash, message, files_changed, author FROM commits").fetchall()
    commit_texts = []
    for r in commit_rows:
        ref_id = r['hash']
        if ref_id not in existing_commit_ids:
            text = prepare_commit_text(r['message'], r['files_changed'], r['author'])
            commit_texts.append(('commit', ref_id, text))

    total_new = len(func_texts) + len(commit_texts)
    total_existing = len(existing_func_ids) + len(existing_commit_ids)

    if total_new == 0:
        console.print(f"  Embeddings up to date ({total_existing} total)")
        return

    console.print(f"  Functions: {len(func_texts)} new ({len(existing_func_ids)} existing)")
    console.print(f"  Commits: {len(commit_texts)} new ({len(existing_commit_ids)} existing)")

    # Batch embed and store
    batch_size = 64
    all_items = func_texts + commit_texts
    embedded_count = 0
    failed_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("  Embedding", total=len(all_items))

        for i in range(0, len(all_items), batch_size):
            batch = all_items[i:i + batch_size]
            texts = [item[2] for item in batch]

            vectors = client.embed_batch(texts)
            if vectors is None:
                failed_count += len(batch)
                progress.advance(task, len(batch))
                continue

            entries = []
            for j, item in enumerate(batch):
                type_, ref_id, text = item
                entries.append((type_, ref_id, text, vectors[j].tobytes(), embed_model))
            store.save_embeddings_batch(entries)
            embedded_count += len(batch)
            progress.advance(task, len(batch))

    # Update meta
    store.set_meta('embed_model', embed_model)
    if client.dim:
        store.set_meta('embed_dim', str(client.dim))

    total = total_existing + embedded_count
    store.set_meta('embed_count', str(total))
    msg = f"  Stored {embedded_count} new embeddings ({total} total)"
    if failed_count:
        msg += f" [yellow]({failed_count} failed)[/yellow]"
    console.print(msg)


def _full_index(engine, store, parser, blame_analyzer, commits, max_commits, head_hash):
    """Run a full (non-incremental) index."""
    import time as _time

    store.clear_all()

    # Phase 1: Store commits
    console.print("\n[cyan]Phase 1:[/cyan] Storing commits...")
    for c in commits:
        store.save_commit(c)
    store.flush()
    console.print(f"  Stored {len(commits)} commits")

    # Phase 2: Parse functions at HEAD
    console.print("[cyan]Phase 2:[/cyan] Analyzing files and functions...")
    tracked_files = engine.get_tracked_files()
    console.print(f"  Found {len(tracked_files)} supported files")

    functions_by_file: dict = {}
    total_functions = 0
    for file_path in tracked_files:
        ext = os.path.splitext(file_path)[1]
        language = engine.SUPPORTED_EXTENSIONS.get(ext)
        if not language:
            continue
        source = engine.get_file_at_commit(head_hash, file_path)
        if not source:
            continue
        functions = parser.parse_file(source, file_path, language)
        if functions:
            functions_by_file[file_path] = functions
            for func in functions:
                store.save_function(func)
            total_functions += len(functions)

    store.flush()
    workers = min(8, max(1, os.cpu_count() or 4))
    console.print(f"  Found {total_functions} functions/classes")

    # Phase 3: Blame analysis (parallelized)
    console.print("[cyan]Phase 3:[/cyan] Analyzing blame data...")
    blame_files = [fp for fp in tracked_files if fp in functions_by_file]

    def _blame_one_file(file_path):
        blame_data = engine.get_blame_for_file(file_path)
        if not blame_data:
            return []
        results = []
        for func in functions_by_file[file_path]:
            results.extend(blame_analyzer.analyze_function_blame(blame_data, func))
        return results

    blame_count = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_blame_one_file, fp) for fp in blame_files]
        for future in as_completed(futures):
            entries = future.result()
            for entry in entries:
                store.save_blame_entry(entry)
            blame_count += len(entries)

    store.flush()
    console.print(f"  Generated {blame_count} blame entries")

    # Phase 4: Track function changes
    console.print("[cyan]Phase 4:[/cyan] Tracking function changes...")
    change_count = _run_phase4(engine, store, parser, commits, max_commits)
    console.print(f"  Tracked {change_count} function changes")

    # Phase 4b: Track config file changes
    console.print("[cyan]Phase 4b:[/cyan] Tracking config changes...")
    config_count = _run_config_phase(engine, store, commits, max_commits)
    console.print(f"  Tracked {config_count} config key changes")

    # Phase 4c: Track dependency changes
    console.print("[cyan]Phase 4c:[/cyan] Tracking dependency changes...")
    dep_count = _run_dep_phase(engine, store, commits, max_commits)
    console.print(f"  Tracked {dep_count} dependency changes")

    # Phase 5: Build FTS5 search index
    console.print("[cyan]Phase 5:[/cyan] Building search index...")
    doc_count = store.rebuild_search_index()
    console.print(f"  Indexed {doc_count} documents")

    # Update meta
    store.set_meta('last_indexed_commit', head_hash)
    store.set_meta('index_timestamp', str(int(_time.time())))

    return store.get_stats()


def _incremental_index(engine, store, parser, blame_analyzer, new_commits,
                       changed_files, max_commits, head_hash, last_indexed):
    """Run an incremental index update."""
    import time as _time

    # Phase 1: Store new commits
    console.print("\n[cyan]Phase 1:[/cyan] Storing new commits...")
    for c in new_commits:
        store.save_commit(c)
    store.flush()
    console.print(f"  Added {len(new_commits)} new commits")

    # Phase 2: Re-parse changed files at HEAD
    console.print("[cyan]Phase 2:[/cyan] Updating changed files...")
    functions_by_file: dict = {}
    total_functions = 0
    for file_path in changed_files:
        ext = os.path.splitext(file_path)[1]
        language = engine.SUPPORTED_EXTENSIONS.get(ext)
        if not language:
            continue
        # Remove old data for this file
        store.delete_file_data(file_path)
        source = engine.get_file_at_commit(head_hash, file_path)
        if not source:
            continue
        functions = parser.parse_file(source, file_path, language)
        if functions:
            functions_by_file[file_path] = functions
            for func in functions:
                store.save_function(func)
            total_functions += len(functions)

    store.flush()
    console.print(f"  Updated {len(changed_files)} files ({total_functions} functions)")

    # Phase 3: Re-blame changed files (parallelized)
    console.print("[cyan]Phase 3:[/cyan] Updating blame for changed files...")
    blame_files = [fp for fp in changed_files if fp in functions_by_file]

    def _blame_one_file(file_path):
        blame_data = engine.get_blame_for_file(file_path)
        if not blame_data:
            return []
        results = []
        for func in functions_by_file[file_path]:
            results.extend(blame_analyzer.analyze_function_blame(blame_data, func))
        return results

    blame_count = 0
    workers = min(8, max(1, os.cpu_count() or 4))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_blame_one_file, fp) for fp in blame_files]
        for future in as_completed(futures):
            entries = future.result()
            for entry in entries:
                store.save_blame_entry(entry)
            blame_count += len(entries)

    store.flush()
    console.print(f"  Updated {blame_count} blame entries")

    # Phase 4: Track function changes for new commits only
    console.print("[cyan]Phase 4:[/cyan] Tracking function changes...")
    change_count = _run_phase4(engine, store, parser, new_commits, max_commits)
    console.print(f"  Tracked {change_count} function changes")

    # Phase 4b: Track config file changes
    console.print("[cyan]Phase 4b:[/cyan] Tracking config changes...")
    config_count = _run_config_phase(engine, store, new_commits, max_commits)
    console.print(f"  Tracked {config_count} config key changes")

    # Phase 4c: Track dependency changes
    console.print("[cyan]Phase 4c:[/cyan] Tracking dependency changes...")
    dep_count = _run_dep_phase(engine, store, new_commits, max_commits)
    console.print(f"  Tracked {dep_count} dependency changes")

    # Phase 5: Rebuild FTS5
    console.print("[cyan]Phase 5:[/cyan] Rebuilding search index...")
    doc_count = store.rebuild_search_index()
    console.print(f"  Indexed {doc_count} documents")

    # Update meta
    store.set_meta('last_indexed_commit', head_hash)
    store.set_meta('index_timestamp', str(int(_time.time())))

    return store.get_stats()


@main.command()
@click.argument('path', default='.')
@click.option('--max-commits', type=int, default=None, help='Limit commits to analyze (default: all)')
@click.option('--force', is_flag=True, default=False, help='Force full reindex from scratch')
@click.option('--no-semantic', is_flag=True, default=False, help='Skip embedding phase')
@click.option('--embed-model', default=None,
              help='Embedding model name (auto-detected if not set)')
@click.option('--embed-endpoint', default=None, help='Override embedding API endpoint')
def index(path: str, max_commits: Optional[int], force: bool,
          no_semantic: bool, embed_model: str, embed_endpoint: str):
    """Index a git repository for search.

    By default, performs an incremental update if an existing index is found.
    Use --force for a full reindex from scratch.
    Embeddings are built automatically unless --no-semantic is set.

    \b
    Examples:
      gitast index
      gitast index /path/to/repo
      gitast index . --max-commits 50
      gitast index . --force
      gitast index . --no-semantic
    """
    if max_commits is not None and max_commits < 1:
        console.print("[red]--max-commits must be a positive integer[/red]")
        raise SystemExit(1)
    path = _resolve_path(path)
    engine = _get_engine(path)
    repo_name = engine.get_repo_name()
    db_path = os.path.join(path, DEFAULT_DB)
    try:
        head_hash = engine.repo.head.commit.hexsha
    except ValueError:
        console.print("[red]Repository has no commits. Nothing to index.[/red]")
        raise SystemExit(1)

    console.print(f"[bold]Indexing repository:[/bold] {repo_name}")

    parser = ASTParser()
    blame_analyzer = BlameAnalyzer()

    # Determine full vs incremental
    do_full = force or not os.path.exists(db_path)

    with DataStore(db_path) as store:
        store.create_schema()

        if not do_full:
            last_indexed = store.get_meta('last_indexed_commit')
            if not last_indexed:
                do_full = True
            elif last_indexed == head_hash:
                console.print("[green]Index is up to date.[/green]")
                display_index_stats(store.get_stats())
                return
            elif not engine.is_ancestor(last_indexed, head_hash):
                console.print("[yellow]Branch history changed. Performing full reindex.[/yellow]")
                do_full = True

        try:
            if do_full:
                if force:
                    console.print("[dim]Full reindex requested.[/dim]")
                commits = engine.extract_commits(max_count=max_commits)
                if not commits:
                    console.print("[yellow]No commits found.[/yellow]")
                    return
                console.print(f"  Found {len(commits)} commits")
                stats = _full_index(engine, store, parser, blame_analyzer,
                                    commits, max_commits, head_hash)
            else:
                # Incremental
                console.print(f"[dim]Incremental update from {last_indexed[:8]}[/dim]")

                # Get new commits (newest first)
                new_commits = []
                for c in engine.repo.iter_commits(f'{last_indexed}..HEAD'):
                    stats_files = dict(c.stats.files)
                    engine._stats_cache[c.hexsha] = stats_files
                    new_commits.append(GitCommit(
                        hash=c.hexsha, author=c.author.name,
                        timestamp=datetime.fromtimestamp(c.authored_date),
                        message=c.message.strip(), files_changed=len(stats_files),
                    ))
                if not new_commits:
                    console.print("[green]Index is up to date.[/green]")
                    display_index_stats(store.get_stats())
                    return

                console.print(f"  Found {len(new_commits)} new commits")

                # Get files changed between last indexed and HEAD
                changed_files = engine.get_files_changed_between(last_indexed, head_hash)
                console.print(f"  {len(changed_files)} files changed")

                stats = _incremental_index(engine, store, parser, blame_analyzer,
                                           new_commits, changed_files, max_commits,
                                           head_hash, last_indexed)

        except (KeyboardInterrupt, Exception) as exc:
            if store.conn:
                store.conn.rollback()
            if isinstance(exc, KeyboardInterrupt):
                console.print("\n[yellow]Indexing interrupted.[/yellow]")
                raise SystemExit(1)
            raise

        # Phase 6: Semantic embeddings
        if not no_semantic:
            try:
                _run_embedding_phase(store, embed_model, embed_endpoint, force=force)
            except Exception as e:
                console.print(f"  [yellow]Embedding phase failed: {e}[/yellow]")

    console.print(f"\n[bold green]Indexing complete.[/bold green]")
    display_index_stats(stats)


@main.command()
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--force', is_flag=True, default=False, help='Re-embed everything')
@click.option('--model', default=None,
              help='Embedding model name (auto-detected if not set)')
@click.option('--endpoint', default=None, help='Override embedding API endpoint')
def embed(path: str, force: bool, model: str, endpoint: str):
    """Build or rebuild semantic embeddings.

    Embeds functions and commits for semantic search. Only processes
    new entries unless --force is set. Changing --model triggers full re-embed.

    \b
    Examples:
      gitast embed
      gitast embed --force
      gitast embed --model text-embedding-nomic-embed-text-v1.5
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        store.create_schema()  # ensure embeddings table exists for pre-v0.6 indexes
        _run_embedding_phase(store, model, endpoint, force=force)


@main.command()
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def status(path: str, json_out: bool):
    """Show index freshness and statistics.

    Displays when the index was last updated, how many commits behind
    HEAD it is, and summary statistics.

    \b
    Examples:
      gitast status
      gitast status -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        last_indexed = store.get_meta('last_indexed_commit')
        index_ts = store.get_meta('index_timestamp')
        stats = store.get_stats()
        stats['embedding_stats'] = store.get_embedding_stats()

    if json_out:
        click.echo(json.dumps({
            'last_indexed_commit': last_indexed,
            'index_timestamp': index_ts,
            'stats': stats,
        }, default=str, indent=2))
        return

    if not last_indexed:
        console.print("[yellow]Index exists but has no commit bookmark. Run 'gitast index --force'.[/yellow]")
        display_index_stats(stats)
        return

    display_status(path, last_indexed, index_ts, stats)


@main.command()
@click.argument('query')
@click.option('--limit', '-k', default=20, show_default=True, help='Max results to return')
@click.option('--type', '-t', 'type_filter', type=click.Choice(['commit', 'function', 'change']),
              default=None, help='Filter results by type')
@click.option('--semantic', '-s', is_flag=True, default=False, help='Semantic search only (vector similarity)')
@click.option('--exact', '-e', is_flag=True, default=False, help='Keyword search only (FTS5)')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def search(query: str, limit: int, type_filter: str, semantic: bool, exact: bool,
           path: str, json_out: bool):
    """Search over commits, functions, and changes.

    Default: hybrid search (FTS5 + semantic, merged via Weighted RRF).
    Use --semantic for vector-only or --exact for keyword-only.

    \b
    Examples:
      gitast search "authentication"
      gitast search "email streaming bridge"
      gitast search "DataStore" -k 10 --exact
      gitast search "memory management" --semantic
    """
    if semantic and exact:
        console.print("[red]Cannot use both --semantic and --exact[/red]")
        raise SystemExit(1)

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        if exact:
            # FTS5 only (original behavior)
            results = store.search(query, limit=limit, type_filter=type_filter)
            for r in results:
                r['source'] = 'exact'
                r['relevance'] = ''
        elif semantic:
            # Semantic only
            if not store.has_embeddings():
                console.print("[yellow]No embeddings found. Run 'gitast index' with embedding endpoint available.[/yellow]")
                results = store.search(query, limit=limit, type_filter=type_filter)
                for r in results:
                    r['source'] = 'exact'
                    r['relevance'] = ''
            else:
                embed_model = store.get_meta('embed_model')
                client = EmbeddingClient(model=embed_model)
                qvec = client.embed_single(query)
                if qvec is not None:
                    results = store.semantic_search(qvec, limit=limit, type_filter=type_filter)
                else:
                    console.print("[yellow]Embedding endpoint unavailable. Falling back to keyword search.[/yellow]")
                    results = store.search(query, limit=limit, type_filter=type_filter)
                    for r in results:
                        r['source'] = 'exact'
                        r['relevance'] = ''
        else:
            # Hybrid (default)
            if store.has_embeddings():
                fts5_w, sem_w = classify_query(query)
                embed_model = store.get_meta('embed_model')
                client = EmbeddingClient(model=embed_model)
                qvec = client.embed_single(query)
                results = store.hybrid_search(
                    query, qvec, limit=limit, type_filter=type_filter,
                    fts5_weight=fts5_w, semantic_weight=sem_w,
                )
            else:
                # No embeddings — silent fallback to FTS5
                results = store.search(query, limit=limit, type_filter=type_filter)
                for r in results:
                    r['source'] = 'exact'
                    r['relevance'] = ''

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_search_results(results, query)


@main.command()
@click.argument('function_name')
@click.argument('file_path', required=False, default=None)
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def history(function_name: str, file_path: str, path: str, json_out: bool):
    """Show change history for a function.

    Provide just a function name to search across all files, or add a file
    path to narrow results. If the first argument looks like a file path
    (contains / or .), arguments are swapped for backward compatibility.

    \b
    Examples:
      gitast history DataStore
      gitast history DataStore core.py
      gitast history src/gitast/core.py DataStore
    """
    # Backward compat: if function_name looks like a path, swap args
    if file_path and ('/' not in function_name and '.' not in function_name):
        pass  # correct order: function_name first
    elif '/' in function_name or ('.' in function_name and not function_name[0].isupper()):
        function_name, file_path = file_path, function_name

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        if file_path:
            changes = store.get_function_history(file_path, function_name)
            if not changes:
                changes = store.get_function_history(file_path, function_name, fuzzy_path=True)
        else:
            changes = store.get_function_history_by_name(function_name)

    if json_out:
        from dataclasses import asdict
        click.echo(json.dumps([asdict(c) for c in changes], default=str, indent=2))
        return

    if not changes and file_path:
        display_function_history([], file_path, function_name)
    elif not changes:
        console.print(f"[yellow]No history found for '{function_name}'.[/yellow]")
    else:
        # Group by file if multiple files
        files = sorted(set(c.file_path for c in changes))
        if len(files) > 1:
            for fp in files:
                file_changes = [c for c in changes if c.file_path == fp]
                display_function_history(file_changes, fp, function_name)
        else:
            display_function_history(changes, files[0] if files else '', function_name)


@main.command()
@click.argument('file_path')
@click.argument('function_name')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def blame(file_path: str, function_name: str, path: str, json_out: bool):
    """Show ownership/blame for a function.

    Displays who wrote what percentage of a function based on git blame,
    with the most common commit per author.

    \b
    Examples:
      gitast blame src/gitast/core.py DataStore
      gitast blame auth.py login -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        entries = store.get_function_blame(file_path, function_name)
        if not entries:
            entries = store.get_function_blame(file_path, function_name, fuzzy_path=True)

    if json_out:
        from dataclasses import asdict
        click.echo(json.dumps([asdict(e) for e in entries], default=str, indent=2))
        return

    display_blame(entries, file_path, function_name)


@main.command()
@click.option('--limit', '-k', default=20, show_default=True, help='Number of functions to show')
@click.option('--author', '-a', default=None, help='Filter by author name')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring match)')
@click.option('--since', default=None, help='Only changes after date (2026-01-01 or 30d/6m/1y)')
@click.option('--until', default=None, help='Only changes before date')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def hotspots(limit: int, author: str, file_filter: str, since: str, until: str, path: str, json_out: bool):
    """Show most frequently changed functions.

    Ranks all indexed functions by number of changes, with a breakdown
    of how many times each was added, modified, or deleted, and how many
    authors have touched it.

    \b
    Examples:
      gitast hotspots
      gitast hotspots -k 10
      gitast hotspots --author Alice
      gitast hotspots --since 30d
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)


    with DataStore(db_path) as store:
        results = store.get_hotspots(limit=limit, author=author, file_filter=file_filter, since=since, until=until)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_hotspots(results, author=author or '', file_filter=file_filter or '')


@main.command('blame-summary')
@click.argument('file_path')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def blame_summary(file_path: str, path: str, json_out: bool):
    """Show ownership breakdown for every function in a file.

    Lists all functions/classes/methods in the file with their primary
    owner, ownership percentage, and total number of changes. Good for
    getting an overview of a file before diving into specific functions.

    \b
    Examples:
      gitast blame-summary src/gitast/core.py
      gitast blame-summary auth.py -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_file_blame_summary(file_path)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_blame_summary(results, file_path)


@main.command()
@click.option('--limit', '-k', default=20, show_default=True, help='Number of authors to show')
@click.option('--since', default=None, help='Only changes after date (2026-01-01 or 30d/6m/1y)')
@click.option('--until', default=None, help='Only changes before date')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def authors(limit: int, since: str, until: str, path: str, json_out: bool):
    """Show per-author contribution breakdown.

    Ranks authors by number of function changes, with counts of functions
    and files touched, total lines added, and first contribution date.

    \b
    Examples:
      gitast authors
      gitast authors -k 10
      gitast authors --since 30d
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)


    with DataStore(db_path) as store:
        results = store.get_authors(limit=limit, since=since, until=until)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_authors(results)


@main.command()
@click.option('--limit', '-k', default=20, show_default=True, help='Number of commits to show')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring)')
@click.option('--function', '-n', 'function_filter', default=None, help='Filter by function name (substring)')
@click.option('--author', '-a', default=None, help='Filter by author name (substring)')
@click.option('--grep', '-g', 'message_filter', default=None, help='Filter by commit message (substring)')
@click.option('--since', default=None, help='Only commits after date (2026-01-01 or 30d/6m/1y)')
@click.option('--until', default=None, help='Only commits before date')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def commits(limit: int, file_filter: str, function_filter: str, author: str, message_filter: str, since: str, until: str, path: str, json_out: bool):
    """Browse commit history, with optional filters.

    Without filters shows the most recent commits. With filters, shows only
    commits that touched the specified file, function, or author.

    \b
    Examples:
      gitast commits
      gitast commits --file search_engine.py
      gitast commits --author Alice --since 30d
      gitast commits --grep "email bridge"
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)


    with DataStore(db_path) as store:
        results = store.get_commits(limit=limit, file_filter=file_filter,
                                    function_filter=function_filter, author_filter=author,
                                    message_filter=message_filter, since=since, until=until)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_commits(results, file_filter=file_filter or '',
                    function_filter=function_filter or '', author_filter=author or '')


@main.command()
@click.argument('file_path')
@click.argument('function_name')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def show(file_path: str, function_name: str, path: str, json_out: bool):
    """Show the source code of a function.

    Displays the current function body with syntax highlighting and line
    numbers. Uses fuzzy path matching so the full path isn't required.

    \b
    Examples:
      gitast show src/gitast/core.py DataStore
      gitast show search_engine.py UnifiedSearchEngine
      gitast show core.py get_hotspots -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        func_info = store.get_function_info(file_path, function_name)
        if not func_info:
            func_info = store.get_function_info(file_path, function_name, fuzzy_path=True)

    if not func_info:
        console.print(f"[yellow]Function '{function_name}' not found in '{file_path}'.[/yellow]")
        raise SystemExit(1)

    engine = _get_engine(path)
    try:
        head_commit = engine.repo.head.commit.hexsha
    except Exception:
        console.print("[red]Could not resolve HEAD.[/red]")
        raise SystemExit(1)

    source = engine.get_file_at_commit(head_commit, func_info.file_path)
    if not source:
        console.print(f"[yellow]Could not read '{func_info.file_path}' at HEAD.[/yellow]")
        raise SystemExit(1)

    lines = source.split('\n')
    func_lines = lines[func_info.start_line - 1:func_info.end_line]

    if json_out:
        from dataclasses import asdict
        result = asdict(func_info)
        result['source'] = '\n'.join(func_lines)
        click.echo(json.dumps(result, default=str, indent=2))
        return

    display_show(func_lines, func_info, func_info.file_path)


@main.command('find')
@click.argument('pattern')
@click.option('--kind', '-t', type=click.Choice(['function', 'method', 'class']),
              default=None, help='Filter by kind')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring)')
@click.option('--deleted', '-d', is_flag=True, default=False, help='Show only deleted functions (no longer at HEAD)')
@click.option('--limit', '-k', default=50, show_default=True, help='Max results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def find_func(pattern: str, kind: str, file_filter: str, deleted: bool, limit: int, path: str, json_out: bool):
    """Find functions by name pattern.

    Searches function, method, and class names using substring matching.
    Use --deleted to find functions that were removed from the codebase.

    \b
    Examples:
      gitast find search
      gitast find parse --kind function
      gitast find handler --file api.py -k 10
      gitast find EmailManager --deleted
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    if deleted:
        with DataStore(db_path) as store:
            results = store.get_deleted_functions(limit=limit, pattern=pattern)
        if json_out:
            click.echo(json.dumps(results, default=str, indent=2))
            return
        if not results:
            console.print(f"[yellow]No deleted functions matching '{pattern}'.[/yellow]")
            return
        from rich.table import Table
        table = Table(title=f"Deleted functions matching: {pattern}")
        table.add_column("Function")
        table.add_column("File")
        table.add_column("Deleted")
        table.add_column("Author")
        for r in results:
            deleted_at = r['deleted_at'].strftime('%Y-%m-%d') if r['deleted_at'] else '?'
            table.add_row(r['function_name'], r['file_path'], deleted_at, r['author'])
        console.print(table)
    else:
        with DataStore(db_path) as store:
            results = store.get_functions_by_pattern(pattern, kind=kind,
                                                      file_filter=file_filter, limit=limit)
        if json_out:
            click.echo(json.dumps(results, default=str, indent=2))
            return
        display_find_results(results, pattern)


@main.command()
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring)')
@click.option('--limit', '-k', default=50, show_default=True, help='Max results')
@click.option('--recent', is_flag=True, default=False, help='Show newest-changed first')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def age(file_filter: str, limit: int, recent: bool, path: str, json_out: bool):
    """Show functions sorted by staleness.

    By default shows the oldest-changed functions first (most stale).
    Use --recent to reverse and show newest-changed first.

    \b
    Examples:
      gitast age
      gitast age -k 10
      gitast age --recent --file core.py
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_function_ages(file_filter=file_filter, limit=limit,
                                           recent_first=recent)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_ages(results, recent=recent)


@main.command()
@click.option('--months', '-m', default=12, show_default=True, help='Number of months to show')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def timeline(months: int, path: str, json_out: bool):
    """Show monthly activity chart.

    Displays commits, function changes, unique functions and authors
    per month with an activity bar.

    \b
    Examples:
      gitast timeline
      gitast timeline -m 6
      gitast timeline -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_timeline(months=months)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_timeline(results)


@main.command('diff')
@click.argument('commit')
@click.argument('commit2', required=False, default=None)
@click.option('--filter', '-f', 'diff_filter', default=None, help='Filter by file or function name (substring)')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def diff_cmd(commit: str, commit2: str, diff_filter: str, path: str, json_out: bool):
    """Show function changes in a commit (or between two commits).

    With one argument, shows all function changes in that commit.
    With two arguments, shows changes between the two commits.
    Short hashes (prefix match) and tag names are supported.

    \b
    Examples:
      gitast diff abc123
      gitast diff abc123 def456
      gitast diff v1.0 v2.0
      gitast diff abc123 --filter email
      gitast diff HEAD~1 -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    # Resolve tag names to commit hashes
    commit = _resolve_tag_or_ref(path, commit)
    if commit2:
        commit2 = _resolve_tag_or_ref(path, commit2)

    with DataStore(db_path) as store:
        results = store.get_commit_diff(commit, commit2)

    if diff_filter:
        f = diff_filter.lower()
        results = [r for r in results if f in r.get('file_path', '').lower() or f in r.get('function_name', '').lower()]

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_commit_diff(results, commit, commit2 or '')


@main.command('file')
@click.argument('file_path')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def file_report(file_path: str, path: str, json_out: bool):
    """Show comprehensive report for a file.

    Displays file-level stats (functions, changes, owners) and per-function
    detail including ownership, change count, and age. Supports fuzzy path
    matching so the full path isn't required.

    \b
    Examples:
      gitast file src/gitast/core.py
      gitast file search_engine.py -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        report = store.get_file_report(file_path)

    if not report:
        if json_out:
            click.echo(json.dumps(None))
            return
        console.print(f"[yellow]No functions found for '{file_path}'.[/yellow]")
        raise SystemExit(1)

    if json_out:
        click.echo(json.dumps(report, default=str, indent=2))
        return

    display_file_report(report)


@main.command('export')
@click.argument('format', type=click.Choice(['json', 'csv']))
@click.option('--output', '-o', required=True, type=click.Path(), help='Output file path')
@click.option('--include', '-i', multiple=True,
              type=click.Choice(['functions', 'changes', 'blame', 'authors', 'timeline', 'hotspots']),
              help='Data sections to include (default: all)')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
def export_cmd(format: str, output: str, include: tuple, path: str):
    """Export index data as JSON or CSV.

    \b
    Examples:
      gitast export json -o report.json
      gitast export csv -o data.csv -i functions
      gitast export json -o full.json -i functions -i changes -i blame
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    sections = list(include) if include else None

    with DataStore(db_path) as store:
        data = store.get_export_data(sections)

    output_dir = os.path.dirname(output)
    if output_dir and not os.path.exists(output_dir):
        console.print(f"[red]Output directory does not exist: {output_dir}[/red]")
        raise SystemExit(1)

    if format == 'json':
        with open(output, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        console.print(f"[green]Exported to {output}[/green]")
        for section, items in data.items():
            console.print(f"  {section}: {len(items)} records")
    elif format == 'csv':
        if len(data) == 1:
            # Single section — write directly to the given filename
            section_name = list(data.keys())[0]
            _write_csv(output, data[section_name])
            console.print(f"[green]Exported {section_name} to {output} ({len(data[section_name])} records)[/green]")
        else:
            # Multiple sections — suffix each file
            base, ext = os.path.splitext(output)
            if not ext:
                ext = '.csv'
            for section_name, items in data.items():
                fname = f"{base}_{section_name}{ext}"
                _write_csv(fname, items)
                console.print(f"[green]Exported {section_name} to {fname} ({len(items)} records)[/green]")


def _write_csv(filepath: str, rows: list) -> None:
    """Write a list of dicts to a CSV file."""
    if not rows:
        with open(filepath, 'w') as f:
            pass
        return
    # Sanitize values: convert None to empty string, datetime to ISO format
    clean_rows = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if v is None:
                clean[k] = ''
            elif hasattr(v, 'isoformat'):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        clean_rows.append(clean)
    keys = list(clean_rows[0].keys())
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(clean_rows)


@main.command('cat')
@click.argument('commit')
@click.argument('file_path')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def cat_cmd(commit: str, file_path: str, path: str, json_out: bool):
    """Show file contents at a specific commit.

    Retrieves and displays the full source of a file as it existed at
    the given commit. Supports short hash prefixes. Useful for viewing
    deleted files or past versions.

    \b
    Examples:
      gitast cat abc123 src/email/manager.py
      gitast cat HEAD~5 config.ts
    """
    path = _resolve_path(path)
    engine = _get_engine(path)

    try:
        resolved = engine.repo.commit(commit)
    except Exception:
        console.print(f"[red]Could not resolve commit '{commit}'.[/red]")
        raise SystemExit(1)

    source = engine.get_file_at_commit(resolved.hexsha, file_path)
    if not source:
        console.print(f"[yellow]File '{file_path}' not found at commit {commit}.[/yellow]")
        raise SystemExit(1)

    if json_out:
        click.echo(json.dumps({
            'commit': resolved.hexsha,
            'file': file_path,
            'content': source,
        }, indent=2))
        return

    from rich.syntax import Syntax
    ext = os.path.splitext(file_path)[1].lstrip('.')
    lexer_map = {'py': 'python', 'ts': 'typescript', 'js': 'javascript',
                 'rs': 'rust', 'go': 'go', 'java': 'java', 'c': 'c',
                 'cpp': 'cpp', 'h': 'c', 'hpp': 'cpp'}
    lexer = lexer_map.get(ext, ext or 'text')
    console.print(Syntax(source, lexer, line_numbers=True,
                         theme='monokai', word_wrap=False))


@main.command()
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def langs(path: str, json_out: bool):
    """Show function count per language.

    \b
    Examples:
      gitast langs
      gitast langs -p /path/to/repo
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_language_stats()

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    from rich.table import Table
    table = Table(title="Languages")
    table.add_column("Language", style="bold")
    table.add_column("Functions", justify="right")
    for r in results:
        table.add_row(r['language'], str(r['count']))
    console.print(table)


@main.command()
@click.option('--limit', '-k', default=15, show_default=True, help='Max results')
@click.option('--since', default=None, help='Only changes after date (2026-01-01 or 30d/6m/1y)')
@click.option('--until', default=None, help='Only changes before date')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def fragile(limit: int, since: str, until: str, path: str, json_out: bool):
    """Show fragile zones — functions modified 5+ times.

    These are functions that keep getting reworked, suggesting instability
    or ongoing architectural evolution.

    \b
    Examples:
      gitast fragile
      gitast fragile --since 30d
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)


    with DataStore(db_path) as store:
        results = store.get_fragile_functions(limit=limit, since=since, until=until)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[dim]No fragile zones detected (no functions with 5+ modifications).[/dim]")
        return

    from rich.table import Table
    table = Table(title="Fragile Zones")
    table.add_column("Function", style="bold")
    table.add_column("File")
    table.add_column("Changes", justify="right")
    table.add_column("Authors", justify="right")
    table.add_column("Modifications", justify="right")
    table.add_column("First Change")
    table.add_column("Last Change")
    for r in results:
        first = r['first_change'].strftime('%Y-%m-%d') if r['first_change'] else '?'
        last = r['last_change'].strftime('%Y-%m-%d') if r['last_change'] else '?'
        table.add_row(r['function_name'], r['file_path'],
                      str(r['change_count']), str(r['author_count']),
                      str(r['modify_count']), first, last)
    console.print(table)


@main.command()
@click.option('--limit', '-k', default=15, show_default=True, help='Max results')
@click.option('--since', default=None, help='Only changes after date (2026-01-01 or 30d/6m/1y)')
@click.option('--until', default=None, help='Only changes before date')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def stale(limit: int, since: str, until: str, path: str, json_out: bool):
    """Show stale zones — functions with the oldest last change.

    These are functions that haven't been touched in a long time, suggesting
    dead code, stable utilities, or abandoned features.

    \b
    Examples:
      gitast stale
      gitast stale --since 6m
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)


    with DataStore(db_path) as store:
        results = store.get_stale_functions(limit=limit, since=since, until=until)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[dim]No stale zones detected.[/dim]")
        return

    from rich.table import Table
    table = Table(title="Stale Zones")
    table.add_column("Function", style="bold")
    table.add_column("File")
    table.add_column("Kind")
    table.add_column("Language")
    table.add_column("Last Changed")
    table.add_column("Changes", justify="right")
    for r in results:
        last = r['last_changed'].strftime('%Y-%m-%d') if r['last_changed'] else 'never'
        table.add_row(r['function_name'], r['file_path'], r['kind'], r['language'],
                      last, str(r['total_changes']))
    console.print(table)


@main.command()
@click.option('--limit', '-k', type=int, default=30, show_default=True, help='Number of results')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path substring')
@click.option('--volatile', is_flag=True, help='Show most volatile first (default: most stable)')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def stability(limit: int, file_filter: str, volatile: bool, path: str, json_out: bool):
    """Show function stability scores.

    Ranks functions by a stability score (0.0=volatile, 1.0=stable) based on
    change frequency, recency, and author diversity.

    \b
    Examples:
      gitast stability
      gitast stability --volatile -k 10
      gitast stability -f core.py
    """
    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_stability_scores(limit=limit, file_filter=file_filter)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    display_stability(results, volatile=volatile)


@main.command()
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', is_flag=True, default=False, help='Output as JSON')
def summary(path: str, json_output: bool):
    """One-screen codebase orientation summary.

    Shows key stats, top hotspots, fragile zones, top contributors,
    language breakdown, recent timeline, stability distribution, and
    detected project phase.

    \b
    Examples:
      gitast summary
      gitast summary -p /path/to/repo
      gitast summary --json-output
    """
    from rich.panel import Panel

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        stats = store.get_stats()
        hotspots = store.get_hotspots(limit=5)
        fragile = store.get_fragile_functions(limit=3)
        authors = store.get_authors(limit=3)
        languages = store.get_language_stats()
        timeline = store.get_timeline(months=12)
        stability_scores = store.get_stability_scores(limit=999999)

    # Stability distribution
    dist = {'stable': 0, 'moderate': 0, 'volatile': 0, 'critical': 0}
    for s in stability_scores:
        dist[s['rating']] = dist.get(s['rating'], 0) + 1

    # Detect phase from timeline
    phase = 'unknown'
    if timeline:
        recent = timeline[-3:] if len(timeline) >= 3 else timeline
        avg_changes = sum(t.get('changes', 0) for t in recent) / len(recent)
        avg_authors = sum(t.get('authors', 0) for t in recent) / len(recent)
        if avg_changes > 50 and avg_authors > 2:
            phase = 'active development'
        elif avg_changes > 20:
            phase = 'steady iteration'
        elif avg_changes > 5:
            phase = 'maintenance'
        elif avg_changes > 0:
            phase = 'low activity'
        else:
            phase = 'dormant'

    if json_output:
        data = {
            'stats': stats,
            'hotspots': [{'name': h['function_name'], 'file': h['file_path'], 'changes': h['change_count']} for h in hotspots],
            'fragile_zones': [{'name': f['function_name'], 'file': f['file_path'], 'changes': f['change_count']} for f in fragile],
            'top_authors': [{'author': a['author'], 'changes': a['change_count']} for a in authors],
            'languages': languages,
            'stability_distribution': dist,
            'phase': phase,
        }
        console.print_json(json.dumps(data))
        return

    # Build Rich output
    repo_name = os.path.basename(os.path.abspath(path))
    lines = []

    lines.append(f"[bold]{repo_name}[/bold]  |  "
                 f"{stats['commits']} commits  |  "
                 f"{stats['functions']} functions  |  "
                 f"{stats['changes']} changes  |  "
                 f"Phase: [cyan]{phase}[/cyan]")
    lines.append("")

    lang_str = ", ".join(f"{l['language']}({l['count']})" for l in languages[:6])
    lines.append(f"[bold]Languages:[/bold] {lang_str}")
    lines.append("")

    lines.append(f"[bold]Stability:[/bold] "
                 f"[green]{dist['stable']}[/green] stable  "
                 f"[yellow]{dist['moderate']}[/yellow] moderate  "
                 f"[red]{dist['volatile']}[/red] volatile  "
                 f"[bold red]{dist['critical']}[/bold red] critical")
    lines.append("")

    lines.append("[bold]Top Hotspots:[/bold]")
    for h in hotspots:
        lines.append(f"  {h['function_name']:30s}  {h['file_path']:40s}  {h['change_count']} changes")
    lines.append("")

    if fragile:
        lines.append("[bold]Fragile Zones:[/bold]")
        for f in fragile:
            lines.append(f"  {f['function_name']:30s}  {f['file_path']:40s}  {f['change_count']} changes ({f['modify_count']} mods)")
        lines.append("")

    lines.append("[bold]Top Contributors:[/bold]")
    for a in authors:
        lines.append(f"  {a['author']:30s}  {a['change_count']} changes across {a['files_touched']} files")

    panel = Panel("\n".join(lines), title="Codebase Summary", border_style="cyan")
    console.print(panel)


@main.command()
@click.option('--limit', '-k', type=int, default=10, show_default=True, help='Number of risks to show')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', is_flag=True, default=False, help='Output as JSON')
def risks(limit: int, path: str, json_output: bool):
    """Show prioritised risk assessment for the codebase.

    Combines fragile zones, volatile stability scores, bus factor analysis,
    and high-author-count functions into a scored risk list (0-100).

    \b
    Examples:
      gitast risks
      gitast risks -k 5
      gitast risks --json-output
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        fragile = store.get_fragile_functions(limit=50)
        stability_scores = store.get_stability_scores(limit=999999)
        bus_factors = store.get_bus_factor_by_directory()
        hotspots = store.get_hotspots(limit=50)

    risk_items = []

    # Fragile zones (weight: high)
    max_fragile_changes = max((f['change_count'] for f in fragile), default=1)
    for f in fragile:
        severity = min(100, int(f['change_count'] / max_fragile_changes * 85 + 15))
        risk_items.append({
            'description': f"Fragile function: {f['function_name']}",
            'severity': severity,
            'area': f['file_path'],
            'category': 'fragile',
        })

    # Volatile stability scores (weight: high)
    volatile = [s for s in stability_scores if s['rating'] in ('volatile', 'critical')]
    for v in volatile[:20]:
        severity = min(100, int((1 - v['stability_score']) * 90 + 10))
        risk_items.append({
            'description': f"Volatile function: {v['function_name']}",
            'severity': severity,
            'area': v['file_path'],
            'category': 'volatile',
        })

    # Bus factor (weight: medium)
    for bf in bus_factors:
        severity = min(80, int(bf['percentage'] * 0.7 + 10))
        risk_items.append({
            'description': f"Bus factor: {bf['directory']}/ owned {bf['percentage']}% by {bf['dominant_author']}",
            'severity': severity,
            'area': bf['directory'] + '/',
            'category': 'bus_factor',
        })

    # High author count functions (weight: medium)
    multi_author = [h for h in hotspots if h['author_count'] >= 3]
    for h in multi_author:
        severity = min(75, int(h['author_count'] * 15 + h['change_count'] * 2))
        risk_items.append({
            'description': f"Convergence point: {h['function_name']} ({h['author_count']} authors, {h['change_count']} changes)",
            'severity': severity,
            'area': h['file_path'],
            'category': 'convergence',
        })

    # Sort by severity, take top N
    risk_items.sort(key=lambda x: x['severity'], reverse=True)
    risk_items = risk_items[:limit]

    if json_output:
        for i, r in enumerate(risk_items, 1):
            r['rank'] = i
        console.print_json(json.dumps(risk_items))
        return

    if not risk_items:
        console.print("[green]No significant risks detected.[/green]")
        return

    table = Table(title="Codebase Risk Assessment")
    table.add_column("#", style="bold", width=4)
    table.add_column("Severity", width=10)
    table.add_column("Risk", min_width=40)
    table.add_column("Area", min_width=30)

    for i, r in enumerate(risk_items, 1):
        sev = r['severity']
        if sev >= 75:
            sev_style = "bold red"
        elif sev >= 50:
            sev_style = "yellow"
        else:
            sev_style = "dim"
        table.add_row(str(i), f"[{sev_style}]{sev}[/{sev_style}]", r['description'], r['area'])

    console.print(table)


@main.command()
@click.argument('function_name')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path substring')
@click.option('--limit', '-k', type=int, default=20, show_default=True, help='Max results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def coupled(function_name: str, file_filter: str, limit: int, path: str, json_out: bool):
    """Show functions that frequently change together with the target.

    Coupling analysis finds functions that are modified in the same commits
    as FUNCTION_NAME, revealing hidden dependencies.

    \b
    Examples:
      gitast coupled DataStore
      gitast coupled connect --file core.py
      gitast coupled parse --limit 10 --json-output
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_coupled_functions(function_name, file_path=file_filter, limit=limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print(f"[dim]No coupling data found for '{function_name}'.[/dim]")
        return

    table = Table(title=f"Coupled with '{function_name}'")
    table.add_column("Function", style="bold")
    table.add_column("File")
    table.add_column("Co-changes", justify="right")
    table.add_column("Coupling", justify="right")
    for r in results:
        ratio_str = f"{r['coupling_ratio']:.0%}"
        table.add_row(r['function_name'], r['file_path'],
                      str(r['co_change_count']), ratio_str)
    console.print(table)


@main.command('changed-since')
@click.argument('reference')
@click.option('--limit', '-k', type=int, default=50, show_default=True, help='Max results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def changed_since(reference: str, limit: int, path: str, json_out: bool):
    """Show functions modified since a date or commit.

    REFERENCE can be an ISO date (2026-01-01), relative duration (30d, 6m, 1y),
    or a commit hash.

    \b
    Examples:
      gitast changed-since 30d
      gitast changed-since 2026-01-01
      gitast changed-since abc1234
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    # Try to parse as date first, then as commit hash
    try:
        since_ts = parse_date_filter(reference)
    except ValueError:
        # Try as commit hash
        try:
            engine = _get_engine(path)
            commit = engine.repo.commit(reference)
            since_ts = int(commit.committed_date)
        except Exception:
            console.print(f"[red]Cannot parse '{reference}' as date or commit hash.[/red]")
            raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_changed_functions_since(since_ts, limit=limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print(f"[dim]No functions changed since {reference}.[/dim]")
        return

    table = Table(title=f"Functions changed since {reference}")
    table.add_column("Function", style="bold")
    table.add_column("File")
    table.add_column("Changes", justify="right")
    table.add_column("Last Changed")
    for r in results:
        last = r['last_changed'].strftime('%Y-%m-%d') if r['last_changed'] else '?'
        table.add_row(r['function_name'], r['file_path'],
                      str(r['change_count']), last)
    console.print(table)


@main.command('file-history')
@click.argument('file_path')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def file_history(file_path: str, path: str, json_out: bool):
    """Show lifecycle summary for a file.

    Displays first/last commit, total changes, unique authors, and
    function add/modify/delete counts.

    \b
    Examples:
      gitast file-history src/gitast/core.py
      gitast file-history core.py
    """
    from rich.panel import Panel

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        result = store.get_file_lifecycle(file_path)

    if json_out:
        click.echo(json.dumps(result, default=str, indent=2))
        return

    if not result:
        console.print(f"[dim]No history found for '{file_path}'.[/dim]")
        return

    first = result['first_commit'].strftime('%Y-%m-%d') if result['first_commit'] else '?'
    last = result['last_commit'].strftime('%Y-%m-%d') if result['last_commit'] else '?'
    lines = [
        f"[bold]File:[/bold] {file_path}",
        f"[bold]First commit:[/bold] {first}",
        f"[bold]Last commit:[/bold]  {last}",
        f"[bold]Total changes:[/bold] {result['total_changes']}",
        f"[bold]Unique authors:[/bold] {result['unique_authors']}",
        f"[bold]Unique functions:[/bold] {result['unique_functions']}",
        "",
        f"[green]+{result['functions_added']} added[/green]  "
        f"[yellow]~{result['functions_modified']} modified[/yellow]  "
        f"[red]-{result['functions_deleted']} deleted[/red]",
    ]
    console.print(Panel("\n".join(lines), title="File Lifecycle"))


@main.command()
@click.option('--months', '-m', type=int, default=12, show_default=True, help='Months of history')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def churn(months: int, path: str, json_out: bool):
    """Show churn rate by directory over time.

    Groups function changes by top-level directory and month to identify
    which areas of the codebase are most active.

    \b
    Examples:
      gitast churn
      gitast churn --months 6
      gitast churn --json-output
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_churn_by_directory(months=months)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[dim]No churn data found.[/dim]")
        return

    table = Table(title=f"Churn by Directory (last {months} months)")
    table.add_column("Directory", style="bold")
    table.add_column("Month")
    table.add_column("Changes", justify="right")
    for r in results:
        table.add_row(r['directory'], r['month'], str(r['changes']))
    console.print(table)


@main.command('why')
@click.argument('function_name')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path substring')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def why_cmd(function_name: str, file_filter: str, path: str, json_out: bool):
    """Combined intelligence report for a function.

    Runs history, blame, stability, and coupling analysis in one command
    to give a complete picture of why a function exists in its current form.

    \b
    Examples:
      gitast why DataStore
      gitast why connect --file core.py
      gitast why parse --json-output
    """
    from rich.panel import Panel

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    data = {}
    with DataStore(db_path) as store:
        # History
        if file_filter:
            history = store.get_function_history(file_filter, function_name, fuzzy_path=True)
        else:
            history = store.get_function_history_by_name(function_name)
        data['history'] = [
            {'commit': h.commit_hash[:8], 'type': h.change_type, 'author': h.author,
             'date': h.timestamp.strftime('%Y-%m-%d') if h.timestamp else '?',
             'message': h.message[:80] if h.message else ''}
            for h in history[:10]
        ]

        # Blame
        blame = []
        if file_filter:
            blame_rows = store.get_function_blame(file_filter, function_name, fuzzy_path=True)
            blame = [{'author': b.author, 'lines': b.line_count, 'pct': b.percentage} for b in blame_rows]
        data['blame'] = blame

        # Stability
        all_stability = store.get_stability_scores(limit=999999, file_filter=file_filter)
        fn_stability = [s for s in all_stability if s['function_name'] == function_name]
        data['stability'] = fn_stability[:5]

        # Coupling
        coupled_results = store.get_coupled_functions(function_name, file_path=file_filter, limit=10)
        data['coupled'] = coupled_results

    if json_out:
        click.echo(json.dumps(data, default=str, indent=2))
        return

    lines = []
    lines.append(f"[bold cyan]Function: {function_name}[/bold cyan]")
    if file_filter:
        lines.append(f"[dim]File filter: {file_filter}[/dim]")
    lines.append("")

    # History section
    lines.append("[bold]History[/bold] (last 10 changes)")
    if data['history']:
        for h in data['history']:
            lines.append(f"  {h['date']}  {h['commit']}  [{h['type']}]  {h['author']}  {h['message']}")
    else:
        lines.append("  [dim]No history found[/dim]")
    lines.append("")

    # Blame section
    if data['blame']:
        lines.append("[bold]Blame[/bold]")
        for b in data['blame']:
            lines.append(f"  {b['author']}: {b['lines']} lines ({b['pct']:.0f}%)")
        lines.append("")

    # Stability section
    lines.append("[bold]Stability[/bold]")
    if data['stability']:
        for s in data['stability']:
            lines.append(f"  {s['file_path']}: {s['stability_score']:.2f} ({s['rating']})")
    else:
        lines.append("  [dim]No stability data[/dim]")
    lines.append("")

    # Coupling section
    lines.append("[bold]Coupled functions[/bold]")
    if data['coupled']:
        for c in data['coupled']:
            lines.append(f"  {c['function_name']} ({c['file_path']}) — {c['co_change_count']} co-changes ({c['coupling_ratio']:.0%})")
    else:
        lines.append("  [dim]No coupling data[/dim]")

    console.print(Panel("\n".join(lines), title=f"Why: {function_name}"))


@main.command()
@click.option('--limit', '-k', type=int, default=20, show_default=True, help='Max results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def untested(limit: int, path: str, json_out: bool):
    """Show function changes without corresponding test changes.

    Finds function changes where no test file was also modified in the
    same commit, suggesting potentially untested changes.

    \b
    Examples:
      gitast untested
      gitast untested -k 10
      gitast untested --json-output
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_untested_changes(limit=limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[green]All recent changes have corresponding test modifications.[/green]")
        return

    table = Table(title="Potentially Untested Changes")
    table.add_column("Function", style="bold")
    table.add_column("File")
    table.add_column("Type")
    table.add_column("Author")
    table.add_column("Date")
    table.add_column("Commit")
    for r in results:
        date = r['timestamp'].strftime('%Y-%m-%d') if r['timestamp'] else '?'
        table.add_row(r['function_name'], r['file_path'], r['change_type'],
                      r['author'], date, r['commit_hash'][:8])
    console.print(table)


@main.command()
@click.option('--output', '-o', default='gitast-report.html', show_default=True, help='Output HTML file')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--no-llm', is_flag=True, default=False, help='Skip LLM analysis (charts and tables only)')
@click.option('--remote', is_flag=True, default=False, help='Include remote LLM endpoint in fallback chain')
@click.option('--llm-endpoint', default=None, help='Override primary LLM endpoint')
@click.option('--llm-model', default=None, help='Override LLM model')
def report(output: str, path: str, no_llm: bool, remote: bool, llm_endpoint: str, llm_model: str):
    """Generate an HTML report with timeline visualizations and LLM prose.

    Creates a single self-contained HTML file with interactive Chart.js
    charts showing activity timeline, hotspots, stability, contributors,
    and language breakdown. By default, attempts LLM analysis for narrative
    prose sections; falls back gracefully if unavailable.

    \b
    Examples:
      gitast report
      gitast report -o my-report.html
      gitast report --no-llm
      gitast report --remote --llm-model my-model
      gitast report -p /path/to/repo -o report.html
    """
    from .report import generate_report
    from .analyze import run_analysis
    from .llm import LLMClient, LLMConfig

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[cyan]No index found — indexing first...[/cyan]")
        engine = _get_engine(path)
        repo_name = engine.get_repo_name()
        head_hash = engine.repo.head.commit.hexsha
        parser = ASTParser()
        blame_analyzer = BlameAnalyzer()
        commits = engine.extract_commits()
        if not commits:
            console.print("[yellow]No commits found.[/yellow]")
            raise SystemExit(1)
        console.print(f"  Found {len(commits)} commits")
        with DataStore(db_path) as store:
            store.create_schema()
            _full_index(engine, store, parser, blame_analyzer,
                        commits, None, head_hash)
        console.print()

    with DataStore(db_path) as store:
        data = store.get_report_data()

    # Determine repo name from path
    repo_name = os.path.basename(os.path.abspath(path))

    # Prepare data with Layer 0 extras
    from .report import _prepare_data
    prepared = _prepare_data(data)

    # Add Layer 0 data from DataStore
    with DataStore(db_path) as store:
        prepared['commits_by_month'] = store.get_commits_by_month()
        prepared['fragile_zones'] = store.get_fragile_functions()
        prepared['stale_zones'] = store.get_stale_functions()
        prepared['coauthorship_patterns'] = store.get_coauthorship_patterns()
        prepared['feature_expansion'] = store.get_feature_expansion()

    # Run LLM analysis
    analysis = {}
    if not no_llm:
        config = LLMConfig(use_remote=remote)
        if llm_endpoint:
            config.endpoint = llm_endpoint
        if llm_model:
            config.model = llm_model

        client = LLMClient(config)
        endpoint = client.health_check()
        if endpoint:
            console.print(f"[cyan]LLM connected:[/cyan] {endpoint}")
            analysis = run_analysis(prepared, client, repo_path=path)
            console.print(f"  Generated prose for {len(analysis)} sections")
        else:
            console.print("[dim]LLM unavailable, using fallback summaries[/dim]")
            analysis = run_analysis(prepared, client=None, repo_path=path)
    else:
        # Even with --no-llm, run analysis for Layer 0 data (phases, etc.)
        analysis = run_analysis(prepared, client=None, repo_path=path)

    generate_report(data, repo_name, output, analysis=analysis)
    console.print(f"[green]Report generated: {output}[/green]")

    stats = data['stats']
    console.print(f"  {stats.get('functions', 0)} functions, {stats.get('commits', 0)} commits, {len(data.get('languages', []))} languages")


@main.command('track')
@click.argument('key_path')
@click.argument('file_path', required=False, default=None)
@click.option('--limit', '-l', default=50, show_default=True, help='Maximum results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def track_cmd(key_path: str, file_path: str, limit: int, path: str, json_out: bool):
    """Show change history for a config key path.

    Tracks when specific keys in JSON, YAML, or TOML files were changed,
    who changed them, and what the before/after values were. Supports
    substring matching on key paths.

    \b
    Examples:
      gitast track "btree_score_factor"
      gitast track "database.host" config.yaml
      gitast track "version" package.json --json-output
      gitast track "log_level" -p /path/to/repo
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_config_history(key_path, file_path, limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print(f"[yellow]No config changes found for key '{key_path}'.[/yellow]")
        console.print("[dim]Make sure the repository has been re-indexed after adding config tracking.[/dim]")
        return

    table = Table(title=f"Config history: {key_path}")
    table.add_column("Key", style="bold", max_width=30, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=30, no_wrap=True, overflow='ellipsis')
    table.add_column("Type", width=8, no_wrap=True)
    table.add_column("Old Value", max_width=20, no_wrap=True, overflow='ellipsis')
    table.add_column("New Value", max_width=20, no_wrap=True, overflow='ellipsis')
    table.add_column("Author", max_width=16, no_wrap=True, overflow='ellipsis')
    table.add_column("Commit", width=8, no_wrap=True)
    table.add_column("Date", width=10, no_wrap=True)

    for r in results:
        change_color = {'added': 'green', 'modified': 'yellow', 'deleted': 'red'}.get(r['change_type'], 'white')
        dt = datetime.fromtimestamp(r['timestamp']).strftime('%Y-%m-%d') if r['timestamp'] else '?'
        table.add_row(
            r['key_path'],
            r['file_path'],
            f"[{change_color}]{r['change_type']}[/{change_color}]",
            r.get('old_value', '') or '',
            r.get('new_value', '') or '',
            r['author'],
            r['commit_hash'][:8],
            dt,
        )

    console.print()
    console.print(table)
    console.print(f"\n[dim]{len(results)} changes[/dim]")


@main.command('config-keys')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring)')
@click.option('--limit', '-l', default=100, show_default=True, help='Maximum results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def config_keys_cmd(file_filter: str, limit: int, path: str, json_out: bool):
    """List tracked config keys with change counts.

    Shows all config keys that have been modified across commits, sorted
    by change frequency. Use this to discover which config values change
    most often.

    \b
    Examples:
      gitast config-keys
      gitast config-keys --file config.json
      gitast config-keys -p /path/to/repo
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    with DataStore(db_path) as store:
        results = store.get_config_keys(file_filter, limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[yellow]No config changes tracked yet.[/yellow]")
        console.print("[dim]Re-index with 'gitast index --force' to track config files.[/dim]")
        return

    table = Table(title="Tracked config keys")
    table.add_column("Key Path", style="bold", max_width=40, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=30, no_wrap=True, overflow='ellipsis')
    table.add_column("Changes", justify="right", width=8)
    table.add_column("Last Changed", width=10, no_wrap=True)

    for r in results:
        dt = datetime.fromtimestamp(r['last_changed']).strftime('%Y-%m-%d') if r['last_changed'] else '?'
        table.add_row(
            r['key_path'],
            r['file_path'],
            str(r['change_count']),
            dt,
        )

    console.print()
    console.print(table)
    console.print(f"\n[dim]{len(results)} keys[/dim]")


@main.command('deps')
@click.option('--package', '-k', default=None, help='Filter by package name (substring)')
@click.option('--file', '-f', 'file_filter', default=None, help='Filter by file path (substring)')
@click.option('--added', is_flag=True, default=False, help='Show only added dependencies')
@click.option('--removed', is_flag=True, default=False, help='Show only removed dependencies')
@click.option('--bumped', is_flag=True, default=False, help='Show only version bumps')
@click.option('--summary', '-s', is_flag=True, default=False, help='Show package summary instead of history')
@click.option('--limit', '-l', default=50, show_default=True, help='Maximum results')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def deps_cmd(package: str, file_filter: str, added: bool, removed: bool, bumped: bool,
             summary: bool, limit: int, path: str, json_out: bool):
    """Show dependency change history.

    Tracks additions, removals, and version bumps in package/dependency
    files (requirements.txt, package.json, pyproject.toml, Cargo.toml, go.mod).

    \b
    Examples:
      gitast deps                          # all dependency changes
      gitast deps --added                  # recently added deps
      gitast deps --bumped                 # version bumps only
      gitast deps -k requests              # changes to 'requests'
      gitast deps -f requirements.txt      # changes in requirements.txt
      gitast deps --summary                # package change frequency
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    filter_count = sum([added, removed, bumped])
    if filter_count > 1:
        console.print("[red]Only one of --added, --removed, --bumped can be used at a time.[/red]")
        raise SystemExit(1)

    change_type = None
    if added:
        change_type = 'added'
    elif removed:
        change_type = 'removed'
    elif bumped:
        change_type = 'bumped'

    with DataStore(db_path) as store:
        if summary:
            results = store.get_dep_summary(limit)

            if json_out:
                click.echo(json.dumps(results, default=str, indent=2))
                return

            if not results:
                console.print("[yellow]No dependency changes tracked yet.[/yellow]")
                console.print("[dim]Re-index with 'gitast index --force' to track dependency files.[/dim]")
                return

            table = Table(title="Dependency summary")
            table.add_column("Package", style="bold", max_width=30, no_wrap=True, overflow='ellipsis')
            table.add_column("File", max_width=24, no_wrap=True, overflow='ellipsis')
            table.add_column("Changes", justify="right", width=7)
            table.add_column("Bumps", justify="right", style="yellow", width=5)
            table.add_column("Adds", justify="right", style="green", width=5)
            table.add_column("Removes", justify="right", style="red", width=7)
            table.add_column("Last Changed", width=10, no_wrap=True)

            for r in results:
                dt = datetime.fromtimestamp(r['last_changed']).strftime('%Y-%m-%d') if r['last_changed'] else '?'
                table.add_row(
                    r['package'], r['file_path'],
                    str(r['change_count']), str(r['bumps']),
                    str(r['adds']), str(r['removes']), dt,
                )

            console.print()
            console.print(table)
            console.print(f"\n[dim]{len(results)} packages[/dim]")
            return

        results = store.get_dep_history(package, file_filter, change_type, limit)

    if json_out:
        click.echo(json.dumps(results, default=str, indent=2))
        return

    if not results:
        console.print("[yellow]No dependency changes found.[/yellow]")
        console.print("[dim]Re-index with 'gitast index --force' to track dependency files.[/dim]")
        return

    filter_label = ""
    if package:
        filter_label = f" matching '{package}'"
    if change_type:
        filter_label += f" ({change_type})"

    table = Table(title=f"Dependency changes{filter_label}")
    table.add_column("Package", style="bold", max_width=28, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=22, no_wrap=True, overflow='ellipsis')
    table.add_column("Type", width=8, no_wrap=True)
    table.add_column("Old Version", max_width=16, no_wrap=True, overflow='ellipsis')
    table.add_column("New Version", max_width=16, no_wrap=True, overflow='ellipsis')
    table.add_column("Author", max_width=14, no_wrap=True, overflow='ellipsis')
    table.add_column("Commit", width=8, no_wrap=True)
    table.add_column("Date", width=10, no_wrap=True)

    for r in results:
        change_color = {'added': 'green', 'bumped': 'yellow', 'removed': 'red'}.get(r['change_type'], 'white')
        dt = datetime.fromtimestamp(r['timestamp']).strftime('%Y-%m-%d') if r['timestamp'] else '?'
        table.add_row(
            r['package'], r['file_path'],
            f"[{change_color}]{r['change_type']}[/{change_color}]",
            r.get('old_version', '') or '',
            r.get('new_version', '') or '',
            r['author'],
            r['commit_hash'][:8],
            dt,
        )

    console.print()
    console.print(table)
    console.print(f"\n[dim]{len(results)} changes[/dim]")


@main.command('releases')
@click.option('--path', '-p', default='.', show_default=True, help='Path to indexed repository')
@click.option('--json-output', 'json_out', is_flag=True, default=False, help='Output as JSON')
def releases_cmd(path: str, json_out: bool):
    """Show tagged releases with function change summaries.

    Lists all git tags sorted by date, with the number of function changes
    (added, modified, deleted) between each consecutive pair of tags.

    \b
    Examples:
      gitast releases
      gitast releases -p /path/to/repo
      gitast releases --json-output
    """
    from rich.table import Table

    path = _resolve_path(path)
    db_path = os.path.join(path, DEFAULT_DB)

    if not os.path.exists(db_path):
        console.print("[yellow]No index found. Run 'gitast index .' first.[/yellow]")
        raise SystemExit(1)

    engine = _get_engine(path)
    tags = []
    for tag in engine.repo.tags:
        try:
            commit = tag.commit
            tags.append({
                'name': tag.name,
                'hash': commit.hexsha,
                'timestamp': int(commit.committed_date),
                'author': str(commit.author),
                'message': commit.message.strip().split('\n')[0][:80] if commit.message else '',
            })
        except Exception:
            continue

    if not tags:
        if json_out:
            click.echo(json.dumps([]))
            return
        console.print("[yellow]No tags found in this repository.[/yellow]")
        return

    tags.sort(key=lambda t: t['timestamp'])

    # Get function change summaries between consecutive tags
    with DataStore(db_path) as store:
        for i, tag in enumerate(tags):
            if i == 0:
                summary = store.get_release_diff_summary(None, tag['hash'])
            else:
                summary = store.get_release_diff_summary(tags[i - 1]['hash'], tag['hash'])
            tag['changes'] = summary

    if json_out:
        click.echo(json.dumps(tags, default=str, indent=2))
        return

    table = Table(title="Releases (tagged versions)")
    table.add_column("Tag", style="bold cyan", no_wrap=True)
    table.add_column("Date", no_wrap=True, width=10)
    table.add_column("Author", max_width=16, no_wrap=True, overflow='ellipsis')
    table.add_column("Message", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("+fn", justify="right", style="green", width=4)
    table.add_column("~fn", justify="right", style="yellow", width=4)
    table.add_column("-fn", justify="right", style="red", width=4)
    table.add_column("Files", justify="right", width=5)
    table.add_column("Devs", justify="right", width=4)

    for tag in tags:
        dt = datetime.fromtimestamp(tag['timestamp']).strftime('%Y-%m-%d')
        ch = tag['changes']
        table.add_row(
            tag['name'],
            dt,
            tag['author'],
            tag['message'],
            str(ch['added']),
            str(ch['modified']),
            str(ch['deleted']),
            str(ch['files_touched']),
            str(ch['authors']),
        )

    console.print()
    console.print(table)
    console.print(f"\n[dim]{len(tags)} releases[/dim]")


@main.command('install-hooks')
@click.option('--path', '-p', default='.', show_default=True, help='Path to git repository')
def install_hooks_cmd(path: str):
    """Install git hooks for automatic index updates.

    Adds post-commit and post-merge hooks that run 'gitast index' in the
    background after each commit or merge. Safe with existing hooks — appends
    rather than overwrites.

    \b
    Examples:
      gitast install-hooks
      gitast install-hooks -p /path/to/repo
    """
    from .hooks import install_hooks
    path = _resolve_path(path)

    try:
        installed = install_hooks(path)
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    if installed:
        console.print(f"[green]Installed hooks: {', '.join(installed)}[/green]")
    else:
        console.print("[yellow]All hooks already installed.[/yellow]")


@main.command('uninstall-hooks')
@click.option('--path', '-p', default='.', show_default=True, help='Path to git repository')
def uninstall_hooks_cmd(path: str):
    """Remove GitAST git hooks.

    \b
    Examples:
      gitast uninstall-hooks
      gitast uninstall-hooks -p /path/to/repo
    """
    from .hooks import uninstall_hooks
    path = _resolve_path(path)

    try:
        removed = uninstall_hooks(path)
    except FileNotFoundError as e:
        console.print(f"[red]{e}[/red]")
        raise SystemExit(1)

    if removed:
        console.print(f"[green]Removed hooks: {', '.join(removed)}[/green]")
    else:
        console.print("[yellow]No GitAST hooks found.[/yellow]")


if __name__ == '__main__':
    main()
