"""Display utilities for GitAST using Rich."""
from typing import List, Dict

from rich.console import Console
from rich.table import Table

from .models import FunctionChange, BlameEntry

console = Console()


def _clean_name(name: str, max_len: int = 40) -> str:
    """Sanitize a function name for display: collapse whitespace, truncate."""
    name = ' '.join(name.split())  # collapse all whitespace including newlines
    if len(name) > max_len:
        name = name[:max_len - 1] + '…'
    return name


def _abbrev_path(path: str, max_len: int = 40) -> str:
    """Abbreviate a file path to last 2 components if too long, always within max_len."""
    if len(path) <= max_len:
        return path
    parts = path.replace('\\', '/').split('/')
    if len(parts) >= 2:
        candidate = '…/' + '/'.join(parts[-2:])
        if len(candidate) <= max_len:
            return candidate
        # Even last-2-parts is too long — use just the filename, truncated
        candidate = '…/' + parts[-1]
        if len(candidate) <= max_len:
            return candidate
    return '…' + path[-(max_len - 1):]


def display_search_results(results: List[Dict], query: str) -> None:
    """Display search results in a formatted table with source and relevance indicators."""
    if not results:
        console.print(f"[yellow]No results found for:[/yellow] {query}")
        return

    console.print(f"\n[bold]Search results for:[/bold] {query}\n")

    type_colors = {'commit': 'blue', 'function': 'green', 'change': 'yellow'}
    relevance_colors = {
        'HIGH': 'bold green', 'GOOD': 'cyan', 'FAIR': 'yellow',
        'LOW': 'dim', 'WEAK': 'dim red',
    }
    source_colors = {'hybrid': 'bold magenta', 'semantic': 'blue', 'exact': 'dim'}

    has_source = any(r.get('source') for r in results)
    has_relevance = any(r.get('relevance') for r in results)

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Type", width=10, no_wrap=True)
    table.add_column("Name", max_width=32, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("Detail", min_width=24, no_wrap=True, overflow='ellipsis')
    if has_relevance:
        table.add_column("Rel", width=6, no_wrap=True, justify="right")
    if has_source:
        table.add_column("Via", width=10, no_wrap=True, justify="right")

    for i, r in enumerate(results, 1):
        rtype = r.get('type', '?')
        color = type_colors.get(rtype, 'white')
        name = _clean_name(r.get('name', ''))
        file_path = _abbrev_path(r.get('file_path', '') or '')
        detail = r.get('detail', '')

        row = [
            str(i),
            f"[{color}]{rtype}[/{color}]",
            name,
            file_path,
            detail,
        ]
        if has_relevance:
            rel = r.get('relevance', '')
            rc = relevance_colors.get(rel, 'dim')
            row.append(f"[{rc}]{rel}[/{rc}]" if rel else '')
        if has_source:
            src = r.get('source', '')
            sc = source_colors.get(src, 'dim')
            row.append(f"[{sc}]{src}[/{sc}]" if src else '')

        table.add_row(*row)

    console.print(table)
    console.print(f"\n[dim]{len(results)} results[/dim]")


def display_function_history(changes: List[FunctionChange], file_path: str, func_name: str) -> None:
    """Display function change history as a timeline."""
    if not changes:
        console.print(f"[yellow]No history found for {func_name} in {file_path}[/yellow]")
        return

    console.print(f"\n[bold]History of [cyan]{func_name}[/cyan] in {file_path}[/bold]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Date", width=12, no_wrap=True)
    table.add_column("Author", max_width=24, no_wrap=True, overflow='ellipsis')
    table.add_column("Type", width=10, no_wrap=True)
    table.add_column("+/-", justify="right", width=10, no_wrap=True)
    table.add_column("Commit", width=10, no_wrap=True)
    table.add_column("Message", min_width=30, no_wrap=True, overflow='ellipsis')

    for ch in changes:
        date_str = ch.timestamp.strftime("%Y-%m-%d") if ch.timestamp else "?"
        change_color = {'added': 'green', 'modified': 'yellow', 'deleted': 'red', 'renamed': 'magenta'}.get(ch.change_type, 'white')
        delta = f"[green]+{ch.lines_added}[/green]/[red]-{ch.lines_removed}[/red]"

        type_display = f"[{change_color}]{ch.change_type}[/{change_color}]"
        msg = ch.message.split('\n')[0][:120]
        if ch.change_type == 'renamed' and hasattr(ch, 'renamed_from') and ch.renamed_from:
            msg = f"[magenta][renamed from {ch.renamed_from}][/magenta] {msg}"

        table.add_row(
            date_str,
            ch.author,
            type_display,
            delta,
            ch.commit_hash[:8],
            msg,
        )

    console.print(table)
    console.print(f"\n[dim]{len(changes)} changes[/dim]")


def display_blame(entries: List[BlameEntry], file_path: str, func_name: str) -> None:
    """Display blame/ownership breakdown for a function."""
    if not entries:
        console.print(f"[yellow]No blame data for {func_name} in {file_path}[/yellow]")
        return

    console.print(f"\n[bold]Blame for [cyan]{func_name}[/cyan] in {file_path}[/bold]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Author", max_width=30, no_wrap=True, overflow='ellipsis')
    table.add_column("Lines", justify="right", width=8, no_wrap=True)
    table.add_column("Ownership", justify="right", width=12, no_wrap=True)
    table.add_column("Bar", width=30, no_wrap=True)

    for entry in entries:
        bar_width = int(entry.percentage / 100 * 28)
        bar = "[green]" + "█" * bar_width + "[/green]" + "░" * (28 - bar_width)
        table.add_row(
            entry.author,
            str(entry.line_count),
            f"{entry.percentage:.1f}%",
            bar,
        )

    console.print(table)


def display_hotspots(results: List[Dict], author: str = '', file_filter: str = '') -> None:
    """Display most-changed functions ranked by change count."""
    if not results:
        console.print("[yellow]No function changes found in index.[/yellow]")
        return

    title_parts = ["[bold]Hotspots"]
    if author:
        title_parts.append(f" — author: [cyan]{author}[/cyan]")
    if file_filter:
        title_parts.append(f" — file: [cyan]{file_filter}[/cyan]")
    title_parts.append("[/bold]")
    console.print(f"\n{''.join(title_parts)}\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Function", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=38, no_wrap=True, overflow='ellipsis')
    table.add_column("Changes", justify="right", width=9, no_wrap=True)
    table.add_column("Authors", justify="right", width=9, no_wrap=True)
    table.add_column("+A /M /D", width=14, no_wrap=True)
    table.add_column("Last Changed", width=12, no_wrap=True)

    for i, r in enumerate(results, 1):
        last = r['last_changed'].strftime("%Y-%m-%d") if r['last_changed'] else "?"
        breakdown = (
            f"[green]+{r['added']}[/green] "
            f"[yellow]~{r['modified']}[/yellow] "
            f"[red]-{r['deleted']}[/red]"
        )
        count = r['change_count']
        count_color = 'red' if count > 10 else 'yellow' if count > 4 else 'white'

        table.add_row(
            str(i),
            _clean_name(r['function_name'], max_len=36),
            _abbrev_path(r['file_path'], max_len=38),
            f"[{count_color}]{count}[/{count_color}]",
            str(r['author_count']),
            breakdown,
            last,
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} functions[/dim]")


def display_blame_summary(results: List[Dict], file_path: str) -> None:
    """Display all functions in a file with ownership and change count."""
    if not results:
        console.print(f"[yellow]No functions indexed for {file_path}[/yellow]")
        return

    console.print(f"\n[bold]Blame summary for [cyan]{file_path}[/cyan][/bold]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Lines", justify="right", width=12, no_wrap=True)
    table.add_column("Kind", width=9, no_wrap=True)
    table.add_column("Function", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("Owner", max_width=22, no_wrap=True, overflow='ellipsis')
    table.add_column("Ownership", justify="right", width=10, no_wrap=True)
    table.add_column("Changes", justify="right", width=9, no_wrap=True)

    kind_colors = {'function': 'green', 'method': 'cyan', 'class': 'magenta'}

    for r in results:
        kind_color = kind_colors.get(r['kind'], 'white')
        pct = r['ownership_pct']
        owner = r['primary_owner'] or '[dim]unknown[/dim]'
        count = r['change_count']
        count_color = 'red' if count > 10 else 'yellow' if count > 4 else 'white'

        table.add_row(
            f"{r['start_line']}-{r['end_line']}",
            f"[{kind_color}]{r['kind']}[/{kind_color}]",
            _clean_name(r['name'], max_len=36),
            owner,
            f"{pct:.1f}%" if pct else "—",
            f"[{count_color}]{count}[/{count_color}]" if count else "[dim]0[/dim]",
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} functions[/dim]")


def display_authors(results: List[Dict]) -> None:
    """Display per-author contribution stats."""
    if not results:
        console.print("[yellow]No author data found in index.[/yellow]")
        return

    console.print("\n[bold]Authors[/bold]\n")

    # Total changes for bar scaling
    max_changes = max(r['change_count'] for r in results) or 1

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Author", max_width=28, no_wrap=True, overflow='ellipsis')
    table.add_column("Changes", justify="right", width=9, no_wrap=True)
    table.add_column("Functions", justify="right", width=11, no_wrap=True)
    table.add_column("Files", justify="right", width=7, no_wrap=True)
    table.add_column("+Lines", justify="right", width=8, no_wrap=True)
    table.add_column("Activity", width=24, no_wrap=True)
    table.add_column("Since", width=12, no_wrap=True)

    for i, r in enumerate(results, 1):
        bar_len = max(1, int(r['change_count'] / max_changes * 22))
        bar = "[cyan]" + "█" * bar_len + "[/cyan]" + "░" * (22 - bar_len)
        since = r['first_commit'].strftime("%Y-%m-%d") if r['first_commit'] else "?"

        table.add_row(
            str(i),
            r['author'],
            str(r['change_count']),
            str(r['functions_touched']),
            str(r['files_touched']),
            f"+{r['total_added']}",
            bar,
            since,
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} authors[/dim]")


def display_commits(results: List[Dict], file_filter: str = '',
                    function_filter: str = '', author_filter: str = '') -> None:
    """Display a commit list with optional filter context."""
    if not results:
        console.print("[yellow]No commits found.[/yellow]")
        return

    title = "[bold]Commits"
    if file_filter:
        title += f" — file: [cyan]{file_filter}[/cyan]"
    if function_filter:
        title += f" — function: [cyan]{function_filter}[/cyan]"
    if author_filter:
        title += f" — author: [cyan]{author_filter}[/cyan]"
    title += "[/bold]"
    console.print(f"\n{title}\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Date", width=12, no_wrap=True)
    table.add_column("Commit", width=10, no_wrap=True)
    table.add_column("Author", max_width=22, no_wrap=True, overflow='ellipsis')
    table.add_column("Files", justify="right", width=7, no_wrap=True)
    table.add_column("Message", min_width=40, no_wrap=True, overflow='ellipsis')

    for r in results:
        date_str = r['timestamp'].strftime("%Y-%m-%d") if r['timestamp'] else "?"
        table.add_row(
            date_str,
            r['hash'][:8],
            r['author'],
            str(r['files_changed']),
            r['message'].split('\n')[0][:120],
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} commits[/dim]")


def display_show(source_lines: List[str], func_info, file_path: str) -> None:
    """Display function source with line numbers."""
    from rich.syntax import Syntax

    lang_map = {'python': 'python', 'javascript': 'javascript',
                'typescript': 'typescript', 'tsx': 'tsx',
                'rust': 'rust', 'go': 'go', 'java': 'java',
                'c': 'c', 'cpp': 'cpp'}
    lang = lang_map.get(func_info.language, 'text')

    console.print(
        f"\n[bold]{func_info.kind} [cyan]{func_info.name}[/cyan][/bold]"
        f"  [dim]{file_path} lines {func_info.start_line}–{func_info.end_line}[/dim]\n"
    )

    code = '\n'.join(source_lines)
    syntax = Syntax(code, lang, line_numbers=True,
                    start_line=func_info.start_line, theme="monokai")
    console.print(syntax)


def display_find_results(results: List[Dict], pattern: str) -> None:
    """Display functions matching a name pattern."""
    if not results:
        console.print(f"[yellow]No functions matching:[/yellow] {pattern}")
        return

    console.print(f"\n[bold]Functions matching:[/bold] {pattern}\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Kind", width=9, no_wrap=True)
    table.add_column("Name", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=38, no_wrap=True, overflow='ellipsis')
    table.add_column("Lines", justify="right", width=10, no_wrap=True)
    table.add_column("Signature", min_width=30, no_wrap=True, overflow='ellipsis')

    kind_colors = {'function': 'green', 'method': 'cyan', 'class': 'magenta'}

    for i, r in enumerate(results, 1):
        kc = kind_colors.get(r['kind'], 'white')
        table.add_row(
            str(i),
            f"[{kc}]{r['kind']}[/{kc}]",
            _clean_name(r['name'], max_len=36),
            _abbrev_path(r['file_path'], max_len=38),
            f"{r['start_line']}-{r['end_line']}",
            r.get('signature', '')[:60],
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} functions[/dim]")


def display_ages(results: List[Dict], recent: bool = False) -> None:
    """Display functions sorted by staleness."""
    if not results:
        console.print("[yellow]No function data found.[/yellow]")
        return

    label = "newest" if recent else "stalest"
    console.print(f"\n[bold]Functions by age ({label} first)[/bold]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Name", max_width=32, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("Kind", width=9, no_wrap=True)
    table.add_column("Last Changed", width=12, no_wrap=True)
    table.add_column("Age", justify="right", width=10, no_wrap=True)
    table.add_column("Changes", justify="right", width=9, no_wrap=True)

    kind_colors = {'function': 'green', 'method': 'cyan', 'class': 'magenta'}

    for i, r in enumerate(results, 1):
        days = r['days_ago']
        if days < 0:
            age_str = "[dim]never[/dim]"
        elif days > 365:
            age_str = f"[red]{days}d[/red]"
        elif days > 90:
            age_str = f"[yellow]{days}d[/yellow]"
        elif days < 30:
            age_str = f"[green]{days}d[/green]"
        else:
            age_str = f"{days}d"

        last = r['last_changed'].strftime("%Y-%m-%d") if r['last_changed'] else "[dim]—[/dim]"
        kc = kind_colors.get(r['kind'], 'white')

        table.add_row(
            str(i),
            _clean_name(r['name'], max_len=32),
            _abbrev_path(r['file_path'], max_len=36),
            f"[{kc}]{r['kind']}[/{kc}]",
            last,
            age_str,
            str(r['change_count']),
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} functions[/dim]")


def display_timeline(results: List[Dict]) -> None:
    """Display monthly activity chart."""
    if not results:
        console.print("[yellow]No activity data found.[/yellow]")
        return

    console.print("\n[bold]Monthly Activity Timeline[/bold]\n")

    max_changes = max((r['changes'] for r in results), default=1) or 1

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Month", width=9, no_wrap=True)
    table.add_column("Commits", justify="right", width=9, no_wrap=True)
    table.add_column("Changes", justify="right", width=9, no_wrap=True)
    table.add_column("Functions", justify="right", width=11, no_wrap=True)
    table.add_column("Authors", justify="right", width=9, no_wrap=True)
    table.add_column("Activity", width=30, no_wrap=True)

    for r in results:
        bar_len = max(1, int(r['changes'] / max_changes * 28))
        bar = "[cyan]" + "\u2588" * bar_len + "[/cyan]" + "\u2591" * (28 - bar_len)
        table.add_row(
            r['month'],
            str(r['commits']),
            str(r['changes']),
            str(r['functions']),
            str(r['authors']),
            bar,
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} months[/dim]")


def display_commit_diff(results: List[Dict], commit1: str, commit2: str = '') -> None:
    """Display function changes in a commit or range."""
    if not results:
        label = f"{commit1}..{commit2}" if commit2 else commit1
        console.print(f"[yellow]No function changes found for {label}[/yellow]")
        return

    label = f"{commit1[:8]}..{commit2[:8]}" if commit2 else commit1[:8]
    console.print(f"\n[bold]Function changes in [cyan]{label}[/cyan][/bold]\n")

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", width=4, no_wrap=True)
    table.add_column("Function", max_width=32, no_wrap=True, overflow='ellipsis')
    table.add_column("File", max_width=36, no_wrap=True, overflow='ellipsis')
    table.add_column("Type", width=10, no_wrap=True)
    table.add_column("+/-", justify="right", width=12, no_wrap=True)
    table.add_column("Author", max_width=20, no_wrap=True, overflow='ellipsis')

    for i, r in enumerate(results, 1):
        change_color = {'added': 'green', 'modified': 'yellow', 'deleted': 'red'}.get(r['change_type'], 'white')
        delta = f"[green]+{r['lines_added']}[/green]/[red]-{r['lines_removed']}[/red]"

        table.add_row(
            str(i),
            _clean_name(r['function_name'], max_len=32),
            _abbrev_path(r['file_path'], max_len=36),
            f"[{change_color}]{r['change_type']}[/{change_color}]",
            delta,
            r.get('author', ''),
        )

    console.print(table)
    console.print(f"\n[dim]{len(results)} changes[/dim]")


def display_file_report(report: Dict) -> None:
    """Display comprehensive file report with Rich Panel header."""
    from rich.panel import Panel

    fp = report['file_path']
    stats_text = (
        f"[bold]{fp}[/bold]\n"
        f"Language: [cyan]{report['language']}[/cyan]  "
        f"Functions: [cyan]{report['total_functions']}[/cyan]  "
        f"Changes: [cyan]{report['total_changes']}[/cyan]  "
        f"Owners: [cyan]{report['unique_owners']}[/cyan]"
    )
    console.print()
    console.print(Panel(stats_text, title="File Report", border_style="cyan"))

    funcs = report['functions']
    if not funcs:
        return

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Lines", justify="right", width=12, no_wrap=True)
    table.add_column("Kind", width=9, no_wrap=True)
    table.add_column("Function", max_width=32, no_wrap=True, overflow='ellipsis')
    table.add_column("Owner", max_width=20, no_wrap=True, overflow='ellipsis')
    table.add_column("Own%", justify="right", width=7, no_wrap=True)
    table.add_column("Changes", justify="right", width=9, no_wrap=True)
    table.add_column("Last Changed", width=12, no_wrap=True)
    table.add_column("Age", justify="right", width=8, no_wrap=True)

    kind_colors = {'function': 'green', 'method': 'cyan', 'class': 'magenta'}

    for f in funcs:
        kc = kind_colors.get(f['kind'], 'white')
        owner = f['owner'] or '[dim]unknown[/dim]'
        pct = f"{f['ownership_pct']:.0f}%" if f['ownership_pct'] else "\u2014"
        last = f['last_changed'].strftime("%Y-%m-%d") if f['last_changed'] else "[dim]\u2014[/dim]"
        days = f['days_ago']
        if days < 0:
            age_str = "[dim]never[/dim]"
        elif days > 365:
            age_str = f"[red]{days}d[/red]"
        elif days > 90:
            age_str = f"[yellow]{days}d[/yellow]"
        elif days < 30:
            age_str = f"[green]{days}d[/green]"
        else:
            age_str = f"{days}d"

        cc = f['change_count']
        cc_color = 'red' if cc > 10 else 'yellow' if cc > 4 else 'white'

        table.add_row(
            f"{f['start_line']}-{f['end_line']}",
            f"[{kc}]{f['kind']}[/{kc}]",
            _clean_name(f['name'], max_len=32),
            owner,
            pct,
            f"[{cc_color}]{cc}[/{cc_color}]" if cc else "[dim]0[/dim]",
            last,
            age_str,
        )

    console.print(table)
    console.print(f"\n[dim]{len(funcs)} functions[/dim]")


def display_status(repo_path: str, last_indexed: str, index_ts: str,
                    stats: Dict) -> None:
    """Display index status with freshness info."""
    from rich.panel import Panel
    import time as _time

    # Compute commits behind HEAD
    commits_behind = 0
    try:
        from gitast.core import GitMiningEngine
        engine = GitMiningEngine(repo_path)
        head = engine.repo.head.commit.hexsha
        if head != last_indexed:
            for _ in engine.repo.iter_commits(f'{last_indexed}..HEAD'):
                commits_behind += 1
    except Exception:
        commits_behind = -1  # unknown

    # Format index age
    age_str = "unknown"
    if index_ts:
        elapsed = int(_time.time()) - int(index_ts)
        if elapsed < 60:
            age_str = f"{elapsed}s ago"
        elif elapsed < 3600:
            age_str = f"{elapsed // 60}m ago"
        elif elapsed < 86400:
            age_str = f"{elapsed // 3600}h ago"
        else:
            age_str = f"{elapsed // 86400}d ago"

    # Status line
    if commits_behind == 0:
        freshness = "[green]up to date[/green]"
    elif commits_behind > 0:
        freshness = f"[yellow]{commits_behind} commits behind HEAD[/yellow]"
    else:
        freshness = "[dim]unknown[/dim]"

    # Embedding info
    embed_stats = stats.get('embedding_stats', {})
    if embed_stats:
        embed_line = (
            f"\nEmbeddings: [cyan]{embed_stats['total']}[/cyan] vectors "
            f"({embed_stats['dim']}-dim, {embed_stats['model']})\n"
            f"             Functions: [cyan]{embed_stats['functions']}[/cyan] | "
            f"Commits: [cyan]{embed_stats['commits']}[/cyan]"
        )
    else:
        embed_line = "\nEmbeddings: [dim]None (run 'gitast index' with embedding endpoint available)[/dim]"

    status_text = (
        f"Last indexed: [cyan]{last_indexed[:8]}[/cyan]  ({age_str})\n"
        f"Status: {freshness}\n"
        f"Commits: [cyan]{stats.get('commits', 0)}[/cyan]  "
        f"Functions: [cyan]{stats.get('functions', 0)}[/cyan]  "
        f"Changes: [cyan]{stats.get('changes', 0)}[/cyan]  "
        f"Blame: [cyan]{stats.get('blame_entries', 0)}[/cyan]"
        f"{embed_line}"
    )
    console.print()
    console.print(Panel(status_text, title="Index Status", border_style="cyan"))


def display_stability(results: List[Dict], volatile: bool = False) -> None:
    """Display function stability scores."""
    if not results:
        console.print("[yellow]No function data for stability analysis.[/yellow]")
        return

    if volatile:
        results = list(reversed(results))

    label = "Most Volatile" if volatile else "Most Stable"
    console.print(f"\n[bold]{label} Functions[/bold] — {len(results)} results\n")

    rating_colors = {
        'stable': 'green',
        'moderate': 'yellow',
        'volatile': 'red',
        'critical': 'bold red',
    }

    table = Table(show_lines=False)
    table.add_column("#", style="dim", justify="right", width=4)
    table.add_column("Function", style="cyan", no_wrap=True, overflow='ellipsis')
    table.add_column("File", style="dim", no_wrap=True, overflow='ellipsis')
    table.add_column("Score", justify="right")
    table.add_column("Rating")
    table.add_column("Changes", justify="right")
    table.add_column("Authors", justify="right")
    table.add_column("Last Changed", justify="right")

    for i, r in enumerate(results, 1):
        rating = r['rating']
        color = rating_colors.get(rating, 'white')
        days = r['days_ago']
        age_str = f"{days}d" if days >= 0 else "—"

        table.add_row(
            str(i),
            _clean_name(r['function_name']),
            _abbrev_path(r['file_path']),
            f"{r['stability_score']:.3f}",
            f"[{color}]{rating}[/{color}]",
            str(r['change_count']),
            str(r['author_count']),
            age_str,
        )

    console.print(table)


def display_index_stats(stats: Dict) -> None:
    """Display index statistics."""
    console.print("\n[bold]Index Statistics[/bold]\n")
    table = Table(show_header=False)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right")
    for key, val in stats.items():
        table.add_row(key.replace('_', ' ').title(), str(val))
    console.print(table)
