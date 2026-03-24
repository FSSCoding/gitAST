# GitAST

**Understand any codebase in 60 seconds.**

GitAST reads your git history and builds a function-level map of how your code evolved — who wrote what, what breaks most often, what's been abandoned, and where the risks are hiding. Works offline, runs on any repo, supports 8 languages.

```bash
pip install gitast
gitast index .
gitast summary
```

```
╭────────────────────────────── Codebase Summary ──────────────────────────────╮
│ my-project  |  2,847 commits  |  1,995 functions  |  5,686 changes           │
│ Phase: steady iteration                                                      │
│                                                                              │
│ Stability: 634 moderate  1,242 volatile  37 critical                         │
│                                                                              │
│ Top Hotspots:                                                                │
│   ServiceContainer    src/container.py    23 changes                         │
│   WebScraperAPI       src/scraper.py      23 changes                         │
│                                                                              │
│ Fragile Zones:                                                               │
│   scrape              src/scraper.py      23 changes (22 mods)               │
│                                                                              │
│ Top Contributors:                                                            │
│   alice               4,322 changes across 158 files                         │
│   bob                 1,364 changes across 95 files                          │
╰──────────────────────────────────────────────────────────────────────────────╯
```

## When to use it

- **Inheriting a codebase** — "what is this thing and where are the landmines?"
- **Debugging a regression** — "who changed this function, when, and why?"
- **Planning a refactor** — "what's fragile, what's coupled, what can I safely ignore?"
- **Onboarding** — "give me the 2-minute version of this repo's history"
- **Due diligence** — "how healthy is this codebase before we acquire/depend on it?"

## What it finds

```bash
gitast risks                     # Top 10 risks: fragile code, bus factor, volatile modules
gitast why ServiceContainer      # Full story: history, ownership, stability, coupling
gitast hotspots --since 30d      # Most-changed functions in the last month
gitast fragile                   # Functions that keep getting reworked (instability signal)
gitast coupled scrape            # Functions that always change together (hidden dependencies)
gitast untested                  # Recent changes with no corresponding test changes
```

## What it tracks

Beyond functions, GitAST tracks **config changes** and **dependency evolution**:

```bash
gitast track "db.host"           # When did this config value change? Who changed it?
gitast deps --bumped             # Dependency version bumps across commits
gitast deps --summary            # Which packages churn the most?
gitast releases                  # What changed between tagged versions?
```

Supported: JSON, YAML, TOML configs. requirements.txt, package.json, pyproject.toml, Cargo.toml, go.mod dependencies.

## Search

```bash
gitast search "authentication"                    # Keyword search (FTS5)
gitast search "email streaming bridge" --semantic  # Semantic search (embeddings)
gitast find parse --kind function                  # Find by name pattern
gitast find EmailManager --deleted                 # Find deleted functions
```

Semantic search requires an OpenAI-compatible embedding endpoint. Install with `pip install gitast[embeddings]`.

## HTML Report

```bash
gitast report -o report.html            # Full report with optional LLM narrative
gitast report --no-llm -o report.html   # Charts and tables only
```

Self-contained HTML with interactive charts: timeline, hotspots, stability distribution, feature growth, contributor analysis, fragile/stale zones.

## How it works

1. **Index** — Extracts commits with GitPython. Parses functions/classes/methods with tree-sitter. Maps blame to function boundaries. Tracks changes across commits with rename/move detection. Stores everything in SQLite.

2. **Analyse** — Stability scoring, coupling detection, bus factor calculation, phase detection, churn trending. All deterministic — no AI required for core analysis.

3. **Report** — 37 CLI commands with `--json-output` for agent consumption. Optional LLM narrative synthesis for HTML reports.

## Supported languages

Python, JavaScript, TypeScript, Rust, Go, Java, C, C++ — all via tree-sitter AST parsing.

## Install

```bash
pip install gitast                # Core — 37 commands, 8 languages
pip install gitast[embeddings]    # + semantic search
pip install gitast[all]           # + YAML/TOML config tracking
```

From source:

```bash
git clone https://github.com/FSSCoding/gitAST
cd gitast && pip install -e .[all]
```

## All commands

<details>
<summary>Full command reference (37 commands)</summary>

### Indexing
`index`, `status`, `embed`, `install-hooks`, `uninstall-hooks`

### Search & navigation
`search`, `find`, `history`, `blame`, `blame-summary`, `show`, `cat`

### Codebase analysis
`summary`, `hotspots`, `stability`, `fragile`, `stale`, `risks`, `coupled`, `untested`, `age`, `authors`, `langs`, `timeline`, `churn`, `changed-since`, `why`

### Commit inspection
`commits`, `diff`, `file`, `file-history`

### Release & dependency tracking
`releases`, `deps`, `track`, `config-keys`

### Export & reporting
`export`, `report`

All commands support `--path / -p`, `--json-output`, and most support `--since / --until` time filters.

</details>

## License

MIT — [Brett Fox / Fox Software Solutions](https://foxsoftwaresolutions.com.au)
