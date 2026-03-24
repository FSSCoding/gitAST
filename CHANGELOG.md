# GitAST Changelog

## v0.6.0 (2026-03-24) — Config tracking, dependency evolution, release mapping

### New Commands
- **`gitast releases`** — list all git tags with function change summaries (+added/~modified/-deleted) between consecutive tags
- **`gitast track <key> [file]`** — show change history for config key paths in JSON/YAML/TOML files (who changed, when, before/after values)
- **`gitast config-keys`** — list all tracked config keys sorted by change frequency
- **`gitast deps`** — dependency change history with `--added`, `--removed`, `--bumped`, `--summary` filters
- **`gitast diff`** now resolves tag names (`gitast diff v1.0 v2.0`)

### New Modules
- `config.py` — structured file diff parser for JSON, YAML, TOML with key-path flattening
- `deps.py` — dependency file parsers for requirements.txt, package.json, pyproject.toml, Cargo.toml, go.mod

### Dependency Formats Supported
- Python: requirements.txt, pyproject.toml (PEP 621 + Poetry)
- JavaScript: package.json (dependencies, devDependencies, peerDependencies, optionalDependencies)
- Rust: Cargo.toml
- Go: go.mod

### Bug Fixes (10 edge-case bugs from stress testing)
- Empty repo no longer crashes `index`
- requirements.txt: handle extras brackets, skip git URLs, strip inline comments, fix compound version specs, reject non-package lines
- `export` no longer crashes on invalid output path
- go.mod: strip `// indirect` comments from version strings
- Config parser: handle non-dict YAML/JSON roots (lists, scalars)
- Releases: use `(timestamp, hash)` tuple ordering for same-second tag boundaries
- `deps`: error on conflicting `--added`/`--removed`/`--bumped` flags

### Infrastructure
- New DB tables: `config_changes`, `dep_changes` with indexes and migration
- Indexing pipeline: Phase 4b (config tracking) + Phase 4c (dependency tracking)
- 471 tests (was 397)

---

## v0.5.1 (2026-03-22) — Semantic Search & Codebase Intelligence

### New Commands (14 commands added)
- **`gitast search --semantic`** — vector similarity search with hybrid RRF fusion
- **`gitast embed`** — build/rebuild semantic embeddings (functions + commits)
- **`gitast summary`** — one-screen codebase orientation
- **`gitast risks`** — prioritised risk assessment (fragile functions, bus factor, volatile code)
- **`gitast coupled <function>`** — find functions that co-change together
- **`gitast changed-since <ref>`** — functions modified since a date/commit
- **`gitast file-history <path>`** — file lifecycle summary
- **`gitast churn`** — churn rate by directory over time
- **`gitast why <function>`** — combined intelligence (history + blame + stability + coupling)
- **`gitast untested`** — function changes without corresponding test changes
- **`--since`/`--until`** time-range filters on hotspots, fragile, stale, commits, authors
- **`--json-output`** on all query commands
- **`--grep`** with regex support on commits

### Semantic Search Architecture
- OpenAI-compatible `/v1/embeddings` endpoint
- Functions embedded as `{name} {kind} in {file_path} — {signature}`
- Commits embedded as message text
- Hybrid search: Weighted RRF fusion of FTS5 + cosine similarity
- Embeddings stored as BLOBs in SQLite `embeddings` table
- Auto-detects embedding model and dimensions

### New Features
- **Function rename/move detection** — tracks renames across files using name similarity, body similarity, and signature matching with configurable confidence threshold
- `function_renames` table links old→new function identities across commits
- `history` follows rename chains automatically

---

## v0.5.0 (2026-03-21) — Field Review Features

### New Commands
- **`gitast cat <commit> <file>`** — view file contents at any commit (including deleted files)
- **`gitast langs`** — function count per language
- **`gitast fragile`** — fragile zones: functions modified 5+ times
- **`gitast stale`** — stale zones: functions with oldest last change
- **`gitast commits --grep`** — search commit messages
- **`gitast search --type`** — filter search to commits or functions only
- **`gitast history <name>`** — search by function name across all files
- **`gitast find --deleted`** — find deleted functions no longer at HEAD
- **`gitast diff --filter`** — filter diff by file/function pattern

---

## v0.4.0 (2026-03-20) — Performance & LLM Reliability

### Improvements
- Performance optimizations for large repos (20k+ commits)
- LLM reliability improvements: better JSON repair, retry logic
- OpenClaw showcase support

---

## v0.3.3 (2026-02-26) — Historical Archaeology

### New Features
- **Layered analysis architecture** — 3-layer pipeline: data extraction (Python), pattern analysis (LLM), narrative synthesis (LLM)
- **Phase detection** — automatically identifies development phases (growth, peak, decline, dormant, revival) from timeline activity
- **Commit theme extraction** — classifies commits as fix/feature/refactor/docs/test/config/remove and groups by period
- **Fragile zone detection** — surfaces functions modified 5+ times, indicating ongoing instability
- **Stale zone detection** — identifies forgotten code untouched for extended periods
- **Feature expansion tracking** — monthly cumulative function additions showing when capabilities grew
- **Co-authorship patterns** — functions touched by multiple authors (convergence points)
- **Project context gathering** — extracts README description, doc inventory, changelog milestones
- **Period chapters** — LLM narrates each development phase as a chapter with title and significance
- **Project arc synthesis** — LLM weaves chapters and patterns into a cohesive project story
- **Activity burst detection** — flags periods with >2x average change rate

### New HTML Report Sections
- **Project Arc** — lifecycle narrative with chapter cards showing phase type badges
- **Feature Growth** — cumulative function additions chart with expanding areas table
- **Archaeology** — fragile and stale zones with per-function observations
- **Ownership evolution** — contributor convergence points and ownership shifts in Contributors section
- Phase count added to stat cards

### New DataStore Methods
- `get_commits_by_month()` — all commits grouped by month with messages
- `get_fragile_functions(limit)` — functions with 5+ changes, mostly modifications
- `get_stale_functions(limit)` — functions sorted by oldest last-change
- `get_coauthorship_patterns(limit)` — functions touched by 2+ authors
- `get_feature_expansion()` — functions added per month with expanding directories

### Architecture
- 5 LLM calls (same count as v0.3.2) but vastly richer input data
- Layer 0: all Python computation (phases, themes, fragile/stale, expansion)
- Layer 1: period classification (1 LLM call)
- Layer 2: archaeology + ownership analysis (2 LLM calls)
- Layer 3: project arc + executive summary (2 LLM calls)
- Each layer feeds the next; `--no-llm` fallback preserved for all sections
- `run_analysis()` now accepts `repo_path` for project context gathering
- 316 tests (was 270)

---

## v0.3.2 (2026-02-26) — LLM-Powered Prose Analysis

### New Features
- **LLM prose analysis** in HTML reports — 5 narrative sections generated by LLM:
  - Executive summary: headline, overview, key findings, risk assessment
  - Hotspot analysis: per-function explanations and risk ratings
  - Timeline narrative: project evolution, phases, peak activity
  - Stability assessment: concerns, recommendations
  - Contributors summary: team dynamics, bus factor, highlights
- **`--no-llm`** flag to skip LLM analysis (charts and tables only)
- **`--remote`** flag to include remote LLM endpoint in fallback chain
- **`--llm-endpoint`** / **`--llm-model`** to override LLM configuration
- Graceful fallback: static data-driven summaries when LLM unavailable
- OpenAI-compatible client with endpoint health check and automatic fallback chain
- JSON repair: strips markdown fences, fixes trailing commas, extracts objects from noisy output

### New Modules
- `src/gitast/llm.py` — LLMConfig, LLMClient with retry logic and JSON repair
- `src/gitast/analyze.py` — prompt builders, JSON renderers, and fallbacks for 5 sections

### Architecture
- LLM returns structured JSON, Python renders HTML (LLM never generates markup)
- Endpoint priority: vLLM → LM Studio local → LM Studio remote (with --remote)
- 3 retries per endpoint with exponential backoff and temperature escalation

---

## v0.3.1 (2026-02-25) — HTML Report Generation

### New Commands
- **`gitast report`** — generate a self-contained HTML report with interactive Chart.js visualizations
  - Activity timeline: commits, function changes, and active authors per month
  - Top hotspots: most-changed functions with horizontal bar chart
  - Stability overview: doughnut chart of stable/moderate/volatile/critical distribution
  - Contributors: author rankings with change counts
  - Language breakdown: function count per language (shown when >1 language)
  - Dark/light theme via `prefers-color-scheme`
  - `--output` / `-o` to specify output file (default: `gitast-report.html`)

### Improvements
- Added `get_language_stats()` and `get_report_data()` to DataStore
- 226 tests (was 211)

---

## v0.3.0 (2026-02-25) — Multi-Language, Export, Stability, Hooks

### New Languages
- **Rust** — functions, structs, enums, traits, impl methods
- **Go** — functions, methods with receivers, type declarations
- **Java** — classes, interfaces, enums, methods, constructors
- **C** — functions, structs, enums
- **C++** — classes, methods, structs, enums
- All languages have tree-sitter parsing with regex fallback

### New Commands
- **`gitast export json|csv -o <file>`** — export index data (functions, changes, blame, authors, timeline, hotspots) as JSON or CSV. Supports `--include` to select sections
- **`gitast stability`** — function stability scores (0.0=volatile, 1.0=stable) based on change frequency, recency, and author diversity. `--volatile` flag for reverse sort
- **`gitast install-hooks`** — install post-commit and post-merge hooks for automatic index updates. Safe with existing hooks (appends, never overwrites)
- **`gitast uninstall-hooks`** — cleanly remove GitAST hook sections

### Improvements
- 8 languages supported (was 3): Python, JavaScript, TypeScript, Rust, Go, Java, C, C++
- C/C++ declarator chain unwrapping for correct function name extraction
- 209 tests (was 177)

---

## v0.2.0 (2026-02-25) — Incremental Indexing & Navigation Commands

### New Commands
- **`gitast index . --force`** — full reindex from scratch (previous default behavior)
- **`gitast status`** — show index freshness: last indexed commit, commits behind HEAD, age, stats
- **`gitast find <pattern>`** — find functions by name pattern with `--kind`/`--file` filters
- **`gitast age`** — functions sorted by staleness (oldest-changed first, `--recent` for newest)
- **`gitast timeline`** — monthly activity chart: commits, changes, functions, authors per month
- **`gitast diff <commit>`** — function-level changes in a commit or range
- **`gitast file <path>`** — comprehensive file report with ownership, changes, and age

### Incremental Indexing
- `gitast index .` now only processes new commits since the last index
- Tracks `last_indexed_commit` and `index_timestamp` in meta table
- Changed files are detected via `git diff` and only those are re-parsed and re-blamed
- Branch switches and rebases are detected automatically (triggers full reindex)
- Phase-by-phase flush for crash recovery — interrupted indexes resume from where they stopped
- `--force` flag forces a full reindex from scratch

### Bug Fixes
- `get_file_report` fuzzy match no longer returns functions from multiple files with similar names
- Moved inline `import time` statements to module level
- `display_ages` no longer recreates kind_colors dict on every iteration

---

## v0.1.1 (2026-02-25) — Navigation Expansion + Display Fixes

### New Commands
- **`gitast authors`** — per-author contribution breakdown: change count, functions/files touched, lines added, activity bar, first contribution date
- **`gitast commits`** — browse commit history with `--file`, `--function`, `--author` substring filters
- **`gitast show <file> <func>`** — display function source at HEAD with syntax highlighting and line numbers

### Improvements
- **Fuzzy path matching** — `history`, `blame`, and `show` now fall back to substring path matching when an exact path finds nothing. `search_engine.py UnifiedSearchEngine` works without the full `src/search/search_engine.py` prefix
- **ASTParser name validation** — extracted function names are now validated as proper identifiers (`^[A-Za-z_]\w*$`). Garbage fragments (docstring content, comment text, partial code) are filtered out before entering the database. Re-indexing the Fss-Rag repo dropped from 7,106 phantom "functions" to 4,272 real ones
- **Table display** — all table columns now use `no_wrap=True, overflow='ellipsis'`, eliminating multi-row wrapping of long values
- **Search display** — replaced the FTS5 token-soup Content column with a readable File + Detail layout
- **File path abbreviation** — long paths in hotspots are abbreviated to last 2 path components within terminal width
- **Commit message first-line** — history display strips multiline commit messages to the first line

### Bug Fixes
- History rows no longer wrap into blank continuation rows for long author names or commit messages
- Hotspots no longer show garbled entries like `"ONAL CODE ONLY"` or `"se"` from dirty tree-sitter parsing

---

## v0.1.0 (2026-02-23) — Initial MVP

### Commands
- `gitast index .` — index a repo: commits, functions, blame, function changes, FTS5 search index
- `gitast search "query"` — full-text search with CamelCase/snake_case splitting
- `gitast history <file> <func>` — function change timeline
- `gitast blame <file> <func>` — function ownership breakdown
- `gitast hotspots` — most-changed functions with `--author`/`--file` filters
- `gitast blame-summary <file>` — all functions in a file with ownership and change count

### Stack
- GitPython for git operations
- tree-sitter for Python/JS/TS AST parsing (regex fallback)
- SQLite + FTS5 for storage and full-text search
- Rich for terminal display
- Click for CLI
