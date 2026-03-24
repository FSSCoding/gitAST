"""HTML report generation for GitAST."""
import json
import os
from datetime import datetime
from typing import Dict


def generate_report(data: Dict, repo_name: str, output_path: str,
                    analysis: Dict = None) -> None:
    """Generate a self-contained HTML report with Chart.js visualizations.

    Args:
        data: Output from DataStore.get_report_data()
        repo_name: Repository name for the title
        output_path: Path to write the HTML file
        analysis: Optional LLM analysis dict with prose sections
    """
    # Prepare JSON-safe data
    prepared = _prepare_data(data)
    prepared['analysis'] = analysis or {}
    json_data = json.dumps(prepared, default=str)

    html = REPORT_TEMPLATE.replace('{{REPO_NAME}}', _escape_html(repo_name))
    html = html.replace('{{JSON_DATA}}', json_data)
    html = html.replace('{{GENERATED_AT}}', datetime.now().strftime('%Y-%m-%d %H:%M'))

    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')


def _prepare_data(data: Dict) -> Dict:
    """Convert data to JSON-serializable format."""
    result = {}

    # Stats
    result['stats'] = data.get('stats', {})

    # Timeline — already list of dicts
    result['timeline'] = data.get('timeline', [])

    # Hotspots
    result['hotspots'] = data.get('hotspots', [])

    # Stability — compute distribution counts (only functions with changes)
    stability = data.get('stability', [])
    result['stability'] = stability
    dist = {'stable': 0, 'moderate': 0, 'volatile': 0, 'critical': 0}
    for item in stability:
        if item.get('change_count', 0) > 0:
            rating = item.get('rating', 'stable')
            if rating in dist:
                dist[rating] += 1
    result['stability_dist'] = dist

    # Authors
    result['authors'] = data.get('authors', [])

    # Languages
    result['languages'] = data.get('languages', [])

    return result


REPORT_TEMPLATE = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GitAST Report: {{REPO_NAME}}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=Syne:wght@400;600;800&family=Source+Sans+3:wght@300;400;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #f9f6f1;
  --bg-card: #ffffff;
  --bg-card-hover: #fdf9f5;
  --surface: #f2ede6;
  --surface-2: #e8e1d6;
  --border: #d4c9b8;
  --border-light: #e6dfd3;
  --text: #1a1510;
  --text-secondary: #5a5040;
  --text-muted: #9a8c7a;
  --accent: #E55A2A;
  --accent-light: #f07040;
  --accent-bg: rgba(229, 90, 42, 0.08);
  --accent-bg-hover: rgba(229, 90, 42, 0.14);
  --nav-bg: rgba(249, 246, 241, 0.95);
  --shadow-sm: 0 1px 3px rgba(26, 21, 16, 0.08);
  --shadow-md: 0 4px 16px rgba(26, 21, 16, 0.1);
  --shadow-lg: 0 8px 32px rgba(26, 21, 16, 0.12);
  --font-display: 'Playfair Display', Georgia, 'Times New Roman', serif;
  --font-ui: 'Syne', system-ui, sans-serif;
  --font-body: 'Source Sans 3', system-ui, sans-serif;
  --font-mono: 'Fira Code', 'Cascadia Code', monospace;
  --radius: 4px;
  --radius-lg: 8px;
  --transition: 180ms ease;
  --green: #2d9e2d;
  --yellow: #c99700;
  --orange: #d46b1a;
  --red: #c0392b;
  --blue: #2d7eb5;
  --purple: #7a5baa;
}

body.dark {
  --bg: #0a0805;
  --bg-card: rgba(22, 18, 12, 0.88);
  --bg-card-hover: rgba(30, 25, 17, 0.92);
  --surface: rgba(18, 14, 9, 0.7);
  --surface-2: rgba(26, 21, 14, 0.6);
  --border: rgba(229, 90, 42, 0.2);
  --border-light: rgba(229, 90, 42, 0.12);
  --text: #f0ebe3;
  --text-secondary: #c0b09a;
  --text-muted: #7a6c58;
  --accent: #FF6B35;
  --accent-light: #ff8855;
  --accent-bg: rgba(255, 107, 53, 0.1);
  --accent-bg-hover: rgba(255, 107, 53, 0.18);
  --nav-bg: rgba(10, 8, 5, 0.9);
  --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.4);
  --shadow-md: 0 4px 16px rgba(0, 0, 0, 0.5);
  --shadow-lg: 0 8px 32px rgba(0, 0, 0, 0.6);
  --green: #4aba4a;
  --yellow: #e6a817;
  --orange: #e67e22;
  --red: #e74c3c;
  --blue: #4a9fd4;
  --purple: #9a7bca;
}

body.dark::before,
body.dark::after {
  content: '';
  position: fixed;
  width: 600px;
  height: 600px;
  border-radius: 50%;
  pointer-events: none;
  z-index: 0;
}
body.dark::before {
  top: -200px;
  right: -200px;
  background: radial-gradient(circle, rgba(229, 90, 42, 0.06) 0%, transparent 70%);
}
body.dark::after {
  bottom: -200px;
  left: -200px;
  background: radial-gradient(circle, rgba(229, 90, 42, 0.04) 0%, transparent 70%);
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }

body {
  font-family: var(--font-body);
  font-weight: 400;
  font-size: 15px;
  line-height: 1.65;
  color: var(--text);
  background: var(--bg);
  transition: background var(--transition), color var(--transition);
}

/* Controls */
.controls {
  position: fixed;
  top: 12px;
  right: 16px;
  display: flex;
  gap: 6px;
  z-index: 100;
}
.controls button {
  font-family: var(--font-ui);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  padding: 5px 10px;
  background: var(--bg-card);
  color: var(--text-secondary);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  cursor: pointer;
  transition: all var(--transition);
  box-shadow: var(--shadow-sm);
  backdrop-filter: blur(8px);
}
.controls button:hover {
  border-color: var(--accent);
  color: var(--accent);
  background: var(--accent-bg);
}

/* Navigation */
.nav {
  position: sticky;
  top: 0;
  z-index: 50;
  background: var(--nav-bg);
  border-bottom: 2px solid var(--accent);
  padding: 0 24px;
  display: flex;
  align-items: center;
  gap: 12px;
  min-height: 44px;
  backdrop-filter: blur(12px);
}
.nav-label {
  font-family: var(--font-ui);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--accent);
  flex-shrink: 0;
}
.nav-tabs {
  display: flex;
  gap: 2px;
  overflow-x: auto;
  scrollbar-width: none;
  flex: 1;
}
.nav-tabs::-webkit-scrollbar { display: none; }
.nav-tab {
  font-family: var(--font-ui);
  font-size: 11.5px;
  font-weight: 600;
  color: var(--text-secondary);
  text-decoration: none;
  padding: 10px;
  border-bottom: 2px solid transparent;
  white-space: nowrap;
  transition: all var(--transition);
}
.nav-tab:hover {
  color: var(--accent);
  border-bottom-color: var(--accent);
}

/* Layout */
#content {
  max-width: 1100px;
  margin: 0 auto;
  padding: 0 24px 80px;
  position: relative;
  z-index: 1;
}

/* Header */
header {
  padding: 48px 0 32px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 0;
}
.report-title {
  font-family: var(--font-display);
  font-size: clamp(22px, 4vw, 34px);
  font-weight: 700;
  color: var(--text);
  line-height: 1.2;
  margin-bottom: 8px;
  letter-spacing: -0.01em;
}
.report-meta {
  font-family: var(--font-body);
  font-size: 14px;
  color: var(--text-muted);
}

/* Stat Cards */
.stat-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
  gap: 12px;
  margin: 24px 0;
}
.stat-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  padding: 16px;
  text-align: center;
  transition: all var(--transition);
  box-shadow: var(--shadow-sm);
}
.stat-card:hover {
  border-color: var(--accent);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}
.stat-number {
  font-family: var(--font-display);
  font-size: 28px;
  font-weight: 700;
  color: var(--accent);
  line-height: 1.1;
  display: block;
}
.stat-label {
  font-family: var(--font-ui);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-muted);
  margin-top: 4px;
  display: block;
}

/* Sections */
section {
  margin-top: 0;
  border-bottom: 1px solid var(--border-light);
}
section:last-of-type { border-bottom: none; }

details { width: 100%; }
summary.section-summary {
  list-style: none;
  cursor: pointer;
  padding: 18px 0;
  user-select: none;
  transition: all var(--transition);
}
summary.section-summary::-webkit-details-marker { display: none; }

.section-summary-content {
  display: flex;
  align-items: baseline;
  gap: 12px;
}
summary.section-summary h2 {
  font-family: var(--font-display);
  font-size: 18px;
  font-weight: 700;
  color: var(--text);
  letter-spacing: -0.01em;
  line-height: 1.3;
  transition: color var(--transition);
}
summary.section-summary:hover h2 { color: var(--accent); }
details[open] summary.section-summary h2 { color: var(--accent); }

.section-preview {
  font-family: var(--font-body);
  font-size: 13px;
  color: var(--text-muted);
}
.section-body {
  padding: 0 0 24px;
}

/* Charts */
.chart-container {
  position: relative;
  max-width: 900px;
  margin: 16px 0;
  background: var(--bg-card);
  border: 1px solid var(--border-light);
  border-radius: var(--radius-lg);
  padding: 20px;
  box-shadow: var(--shadow-sm);
}
.chart-container.chart-small {
  max-width: 400px;
}

/* Tables */
table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--font-body);
  font-size: 13px;
  margin: 16px 0;
}
th {
  font-family: var(--font-ui);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-muted);
  padding: 8px 12px;
  text-align: left;
  background: var(--surface);
  border-bottom: 2px solid var(--accent);
}
td {
  padding: 7px 12px;
  border-bottom: 1px solid var(--border-light);
  color: var(--text);
  vertical-align: top;
}
tr:hover td { background: var(--bg-card-hover); }
tr:last-child td { border-bottom: none; }

.rating-stable { color: var(--green); font-weight: 600; }
.rating-moderate { color: var(--yellow); font-weight: 600; }
.rating-volatile { color: var(--orange); font-weight: 600; }
.rating-critical { color: var(--red); font-weight: 700; }
.truncate { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
td.mono { font-family: var(--font-mono); font-size: 12px; }

/* Footer */
footer {
  margin-top: 48px;
  padding: 16px 0;
  border-top: 1px solid var(--border);
  font-family: var(--font-ui);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--text-muted);
  text-align: center;
}

/* Analysis prose */
.analysis-prose {
  background: var(--accent-bg);
  border-left: 3px solid var(--accent);
  border-radius: var(--radius);
  padding: 14px 18px;
  margin: 12px 0 16px;
  font-family: var(--font-body);
  font-size: 14px;
  line-height: 1.7;
  color: var(--text);
}
.analysis-prose p { margin: 6px 0; }
.analysis-prose ul { margin: 6px 0; padding-left: 20px; }
.analysis-prose li { margin: 3px 0; }
.analysis-prose .risk-high { color: var(--red); font-weight: 600; }
.analysis-prose .risk-medium { color: var(--orange); font-weight: 600; }
.analysis-prose .risk-low { color: var(--green); font-weight: 600; }
.analysis-prose .prose-label {
  font-family: var(--font-ui);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--text-muted);
  margin-bottom: 6px;
  display: block;
}

/* Chapter cards */
.chapter-cards {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin: 16px 0;
}
.chapter-card {
  background: var(--bg-card);
  border: 1px solid var(--border-light);
  border-radius: var(--radius-lg);
  padding: 16px 20px;
  box-shadow: var(--shadow-sm);
  transition: all var(--transition);
}
.chapter-card:hover {
  border-color: var(--accent);
  box-shadow: var(--shadow-md);
}
.chapter-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
}
.chapter-period {
  font-family: var(--font-mono);
  font-size: 12px;
  color: var(--text-muted);
}
.chapter-title {
  font-family: var(--font-display);
  font-size: 16px;
  font-weight: 600;
  color: var(--text);
}
.chapter-summary {
  font-family: var(--font-body);
  font-size: 14px;
  color: var(--text-secondary);
  line-height: 1.6;
  margin: 4px 0;
}
.chapter-badge {
  display: inline-block;
  font-family: var(--font-ui);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  padding: 2px 8px;
  border-radius: var(--radius);
  margin-top: 6px;
}
.chapter-badge.growth { background: rgba(45, 158, 45, 0.15); color: var(--green); }
.chapter-badge.peak { background: rgba(229, 90, 42, 0.15); color: var(--accent); }
.chapter-badge.decline { background: rgba(201, 151, 0, 0.15); color: var(--yellow); }
.chapter-badge.dormant { background: rgba(154, 140, 122, 0.15); color: var(--text-muted); }
.chapter-badge.revival { background: rgba(45, 126, 181, 0.15); color: var(--blue); }
.chapter-badge.steady { background: rgba(122, 91, 170, 0.15); color: var(--purple); }

/* Archaeology items */
.archaeology-section {
  margin: 16px 0;
}
.archaeology-section h3 {
  font-family: var(--font-ui);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--text-muted);
  margin: 16px 0 8px;
}
.archaeology-item {
  background: var(--bg-card);
  border: 1px solid var(--border-light);
  border-radius: var(--radius);
  padding: 10px 14px;
  margin: 6px 0;
}
.archaeology-item .arch-name {
  font-family: var(--font-mono);
  font-size: 13px;
  font-weight: 600;
  color: var(--accent);
}
.archaeology-item .arch-file {
  font-family: var(--font-body);
  font-size: 12px;
  color: var(--text-muted);
  margin-left: 8px;
}
.archaeology-item .arch-obs {
  font-family: var(--font-body);
  font-size: 13px;
  color: var(--text-secondary);
  margin-top: 4px;
}

@media print {
  .controls, .nav { display: none; }
  .chart-container { page-break-inside: avoid; }
  details { open: true; }
  summary.section-summary { pointer-events: none; }
}
</style>
</head>
<body>
<div class="controls">
    <button onclick="toggleTheme()">Light / Dark</button>
    <button onclick="expandAll()">Expand All</button>
    <button onclick="collapseAll()">Collapse All</button>
</div>

<nav class="nav">
    <span class="nav-label">GitAST</span>
    <div class="nav-tabs">
        <a href="#executive" class="nav-tab">Summary</a>
        <a href="#project-arc" class="nav-tab">Project Arc</a>
        <a href="#feature-growth" class="nav-tab">Growth</a>
        <a href="#archaeology" class="nav-tab">Archaeology</a>
        <a href="#timeline" class="nav-tab">Timeline</a>
        <a href="#hotspots" class="nav-tab">Hotspots</a>
        <a href="#stability" class="nav-tab">Stability</a>
        <a href="#contributors" class="nav-tab">Contributors</a>
        <a href="#languages" class="nav-tab">Languages</a>
    </div>
</nav>

<div id="content">

<header>
    <div class="report-title">{{REPO_NAME}}</div>
    <div class="report-meta">GitAST Archaeological Report &middot; Generated {{GENERATED_AT}}</div>
</header>

<div class="stat-cards" id="statsCards"></div>

<section id="executive" style="display:none;">
    <details open>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Executive Summary</h2>
                <span class="section-preview" id="executivePreview">Historical overview</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="executiveProse"></div>
        </div>
    </details>
</section>

<section id="project-arc" style="display:none;">
    <details open>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Project Arc</h2>
                <span class="section-preview" id="arcPreview">Lifecycle narrative</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="arcProse"></div>
            <div id="chapterCards" class="chapter-cards"></div>
        </div>
    </details>
</section>

<section id="feature-growth" style="display:none;">
    <details>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Feature Growth</h2>
                <span class="section-preview">How capabilities expanded over time</span>
            </div>
        </summary>
        <div class="section-body">
            <div class="chart-container"><canvas id="featureGrowthChart"></canvas></div>
            <table id="expansionTable"></table>
        </div>
    </details>
</section>

<section id="archaeology" style="display:none;">
    <details>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Archaeology</h2>
                <span class="section-preview">Fragile and stale code zones</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="archaeologyContent"></div>
        </div>
    </details>
</section>

<section id="timeline">
    <details open>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Activity Timeline</h2>
                <span class="section-preview">Commits, changes, and authors per month</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="timelineProse"></div>
            <div class="chart-container"><canvas id="timelineChart"></canvas></div>
        </div>
    </details>
</section>

<section id="hotspots">
    <details open>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Top Hotspots</h2>
                <span class="section-preview">Most frequently changed functions</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="hotspotsProse"></div>
            <div class="chart-container"><canvas id="hotspotsChart"></canvas></div>
            <table id="hotspotsTable"></table>
        </div>
    </details>
</section>

<section id="stability">
    <details>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Stability Overview</h2>
                <span class="section-preview">Function volatility distribution</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="stabilityProse"></div>
            <div class="chart-container chart-small"><canvas id="stabilityChart"></canvas></div>
            <table id="stabilityTable"></table>
        </div>
    </details>
</section>

<section id="contributors">
    <details>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Contributors</h2>
                <span class="section-preview">Author rankings and ownership evolution</span>
            </div>
        </summary>
        <div class="section-body">
            <div id="contributorsProse"></div>
            <div class="chart-container"><canvas id="contributorsChart"></canvas></div>
            <table id="contributorsTable"></table>
        </div>
    </details>
</section>

<section id="languages" style="display:none;">
    <details>
        <summary class="section-summary">
            <div class="section-summary-content">
                <h2>Languages</h2>
                <span class="section-preview">Function count per language</span>
            </div>
        </summary>
        <div class="section-body">
            <div class="chart-container chart-small"><canvas id="languagesChart"></canvas></div>
        </div>
    </details>
</section>

<footer>
    Generated by GitAST
</footer>

</div>

<script>
var DATA = {{JSON_DATA}};
var COLORS = ['#E55A2A','#D4832A','#C9A02A','#5BA35B','#4A8FBA','#7A5BAA','#BA4A6A','#AA6A3A','#3A9A8A','#8A6A5A'];

// Theme management
function isDarkMode() { return document.body.classList.contains('dark'); }
function getChartColors() {
    var dark = isDarkMode();
    return {
        grid: dark ? 'rgba(229,90,42,0.1)' : 'rgba(26,21,16,0.08)',
        text: dark ? '#c0b09a' : '#5a5040',
        accent: dark ? '#FF6B35' : '#E55A2A',
    };
}
function updateChartDefaults() {
    var c = getChartColors();
    Chart.defaults.color = c.text;
    Chart.defaults.borderColor = c.grid;
}

// Restore theme
(function() {
    var saved = localStorage.getItem('gitast-report-theme');
    if (saved === 'dark' || (!saved && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
        document.body.classList.add('dark');
    }
    updateChartDefaults();
})();

function toggleTheme() {
    document.body.classList.toggle('dark');
    localStorage.setItem('gitast-report-theme', isDarkMode() ? 'dark' : 'light');
    updateChartDefaults();
    Chart.helpers.each(Chart.instances, function(chart) {
        chart.options.scales && Object.values(chart.options.scales).forEach(function(s) {
            s.grid = s.grid || {};
            s.grid.color = getChartColors().grid;
            s.ticks = s.ticks || {};
            s.ticks.color = getChartColors().text;
        });
        chart.update();
    });
}
function expandAll() { document.querySelectorAll('section details').forEach(function(d) { d.setAttribute('open',''); }); }
function collapseAll() { document.querySelectorAll('section details').forEach(function(d) { d.removeAttribute('open'); }); }
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

// Stats cards
(function() {
    var s = DATA.stats;
    var langs = DATA.languages ? DATA.languages.length : 0;
    var phases = (DATA.analysis && DATA.analysis.phases) ? DATA.analysis.phases.length : 0;
    var cards = [
        {label: 'Functions', value: s.functions || 0},
        {label: 'Commits', value: s.commits || 0},
        {label: 'Changes', value: s.changes || 0},
        {label: 'Languages', value: langs},
    ];
    if (phases > 0) cards.push({label: 'Phases', value: phases});
    var el = document.getElementById('statsCards');
    el.innerHTML = cards.map(function(c) {
        return '<div class="stat-card"><span class="stat-number">' + c.value.toLocaleString() + '</span><span class="stat-label">' + c.label + '</span></div>';
    }).join('');
})();

// Timeline chart
(function() {
    var tl = DATA.timeline || [];
    if (!tl.length) return;
    var cc = getChartColors();
    new Chart(document.getElementById('timelineChart'), {
        type: 'bar',
        data: {
            labels: tl.map(function(r) { return r.month; }),
            datasets: [
                {
                    label: 'Function Changes',
                    data: tl.map(function(r) { return r.changes || 0; }),
                    backgroundColor: cc.accent + '99',
                    borderColor: cc.accent,
                    borderWidth: 1,
                    borderRadius: 3,
                    order: 2,
                },
                {
                    label: 'Commits',
                    data: tl.map(function(r) { return r.commits || 0; }),
                    type: 'line',
                    borderColor: '#5BA35B',
                    backgroundColor: 'rgba(91,163,91,0.1)',
                    fill: false,
                    tension: 0.3,
                    pointRadius: 4,
                    pointBackgroundColor: '#5BA35B',
                    borderWidth: 2,
                    order: 1,
                },
                {
                    label: 'Active Authors',
                    data: tl.map(function(r) { return r.authors || 0; }),
                    type: 'line',
                    borderColor: '#4A8FBA',
                    backgroundColor: 'rgba(74,143,186,0.1)',
                    fill: false,
                    tension: 0.3,
                    pointRadius: 4,
                    pointBackgroundColor: '#4A8FBA',
                    borderWidth: 2,
                    yAxisID: 'y1',
                    order: 0,
                },
            ]
        },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { labels: { font: { family: "'Syne', sans-serif", size: 11, weight: 600 }, usePointStyle: true, pointStyle: 'circle' } },
            },
            scales: {
                y: { beginAtZero: true, title: { display: true, text: 'Changes / Commits', font: { family: "'Syne', sans-serif", size: 10, weight: 600 } }, grid: { color: cc.grid } },
                y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Authors', font: { family: "'Syne', sans-serif", size: 10, weight: 600 } } },
                x: { ticks: { maxRotation: 45, font: { size: 11 } }, grid: { color: cc.grid } },
            },
        }
    });
})();

// Hotspots chart + table
(function() {
    var hs = (DATA.hotspots || []).slice(0, 10);
    if (!hs.length) return;
    var cc = getChartColors();
    new Chart(document.getElementById('hotspotsChart'), {
        type: 'bar',
        data: {
            labels: hs.map(function(r) { return r.function_name + ' (' + (r.file_path || '').split('/').pop() + ')'; }),
            datasets: [{
                label: 'Changes',
                data: hs.map(function(r) { return r.change_count || 0; }),
                backgroundColor: COLORS.slice(0, hs.length).map(function(c) { return c + 'CC'; }),
                borderColor: COLORS.slice(0, hs.length),
                borderWidth: 1,
                borderRadius: 3,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: cc.grid }, ticks: { font: { size: 11 } } },
                y: { ticks: { font: { family: "'Source Sans 3', sans-serif", size: 12 } }, grid: { display: false } },
            },
        }
    });
    var all = DATA.hotspots || [];
    var tbl = document.getElementById('hotspotsTable');
    var html = '<thead><tr><th>#</th><th>Function</th><th>File</th><th>Changes</th><th>Authors</th></tr></thead><tbody>';
    all.forEach(function(r, i) {
        html += '<tr><td>' + (i+1) + '</td><td class="mono">' + esc(r.function_name) + '</td><td class="truncate">' + esc(r.file_path || '') + '</td><td>' + (r.change_count||0) + '</td><td>' + (r.author_count||0) + '</td></tr>';
    });
    html += '</tbody>';
    tbl.innerHTML = html;
})();

// Stability chart + table
(function() {
    var dist = DATA.stability_dist || {};
    var vals = [dist.stable||0, dist.moderate||0, dist.volatile||0, dist.critical||0];
    if (vals.every(function(v) { return v === 0; })) return;
    new Chart(document.getElementById('stabilityChart'), {
        type: 'doughnut',
        data: {
            labels: ['Stable', 'Moderate', 'Volatile', 'Critical'],
            datasets: [{
                data: vals,
                backgroundColor: ['#5BA35B', '#C9A02A', '#D4832A', '#c0392b'],
                borderColor: isDarkMode() ? '#0a0805' : '#f9f6f1',
                borderWidth: 2,
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom', labels: { font: { family: "'Syne', sans-serif", size: 11, weight: 600 }, usePointStyle: true, pointStyle: 'circle', padding: 16 } },
            },
        },
    });
    var stab = (DATA.stability || []).filter(function(r) { return r.rating !== 'stable'; }).sort(function(a,b) { return (a.stability_score||0) - (b.stability_score||0); }).slice(0, 10);
    if (!stab.length) return;
    var tbl = document.getElementById('stabilityTable');
    var html = '<thead><tr><th>Function</th><th>File</th><th>Score</th><th>Rating</th><th>Changes</th></tr></thead><tbody>';
    stab.forEach(function(r) {
        var cls = 'rating-' + (r.rating || 'stable');
        html += '<tr><td class="mono">' + esc(r.function_name || '') + '</td><td class="truncate">' + esc(r.file_path || '') + '</td><td>' + (r.stability_score !== undefined ? r.stability_score.toFixed(2) : '-') + '</td><td class="' + cls + '">' + esc(r.rating || '') + '</td><td>' + (r.change_count||0) + '</td></tr>';
    });
    html += '</tbody>';
    tbl.innerHTML = html;
})();

// Contributors chart + table
(function() {
    var au = (DATA.authors || []).slice(0, 10);
    if (!au.length) return;
    var cc = getChartColors();
    new Chart(document.getElementById('contributorsChart'), {
        type: 'bar',
        data: {
            labels: au.map(function(r) { return r.author; }),
            datasets: [{
                label: 'Changes',
                data: au.map(function(r) { return r.change_count || 0; }),
                backgroundColor: COLORS.slice(0, au.length).map(function(c) { return c + 'CC'; }),
                borderColor: COLORS.slice(0, au.length),
                borderWidth: 1,
                borderRadius: 3,
            }]
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                x: { grid: { color: cc.grid }, ticks: { font: { size: 11 } } },
                y: { ticks: { font: { family: "'Syne', sans-serif", size: 12, weight: 600 } }, grid: { display: false } },
            },
        }
    });
    var all = DATA.authors || [];
    var tbl = document.getElementById('contributorsTable');
    var html = '<thead><tr><th>Author</th><th>Changes</th><th>Functions</th><th>Files</th><th>Since</th></tr></thead><tbody>';
    all.forEach(function(r) {
        var since = r.first_commit || '';
        if (since && since.length > 10) since = since.substring(0, 10);
        html += '<tr><td>' + esc(r.author) + '</td><td>' + (r.change_count||0) + '</td><td>' + (r.functions_touched||0) + '</td><td>' + (r.files_touched||0) + '</td><td>' + esc(since) + '</td></tr>';
    });
    html += '</tbody>';
    tbl.innerHTML = html;
})();

// Languages chart
(function() {
    var langs = DATA.languages || [];
    if (langs.length < 2) return;
    document.getElementById('languages').style.display = '';
    new Chart(document.getElementById('languagesChart'), {
        type: 'doughnut',
        data: {
            labels: langs.map(function(r) { return r.language; }),
            datasets: [{
                data: langs.map(function(r) { return r.count; }),
                backgroundColor: COLORS.slice(0, langs.length).map(function(c) { return c + 'CC'; }),
                borderColor: isDarkMode() ? '#0a0805' : '#f9f6f1',
                borderWidth: 2,
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'bottom', labels: { font: { family: "'Syne', sans-serif", size: 11, weight: 600 }, usePointStyle: true, pointStyle: 'circle', padding: 16 } },
            },
        },
    });
})();

// Feature Growth chart
(function() {
    var expansion = (DATA.analysis && DATA.analysis.feature_expansion) || [];
    if (!expansion.length) return;
    document.getElementById('feature-growth').style.display = '';
    var cc = getChartColors();
    new Chart(document.getElementById('featureGrowthChart'), {
        type: 'bar',
        data: {
            labels: expansion.map(function(e) { return e.month; }),
            datasets: [
                {
                    label: 'New Functions',
                    data: expansion.map(function(e) { return e.new_functions || 0; }),
                    backgroundColor: cc.accent + '99',
                    borderColor: cc.accent,
                    borderWidth: 1,
                    borderRadius: 3,
                    order: 2,
                },
                {
                    label: 'Cumulative',
                    data: expansion.map(function(e) { return e.cumulative_functions || 0; }),
                    type: 'line',
                    borderColor: '#5BA35B',
                    backgroundColor: 'rgba(91,163,91,0.08)',
                    fill: true,
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: '#5BA35B',
                    borderWidth: 2,
                    yAxisID: 'y1',
                    order: 1,
                },
            ]
        },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { labels: { font: { family: "'Syne', sans-serif", size: 11, weight: 600 }, usePointStyle: true, pointStyle: 'circle' } },
            },
            scales: {
                y: { beginAtZero: true, title: { display: true, text: 'New Functions / Month', font: { family: "'Syne', sans-serif", size: 10, weight: 600 } }, grid: { color: cc.grid } },
                y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Cumulative', font: { family: "'Syne', sans-serif", size: 10, weight: 600 } } },
                x: { ticks: { maxRotation: 45, font: { size: 11 } }, grid: { color: cc.grid } },
            },
        }
    });
    // Expansion areas table
    var hasAreas = expansion.some(function(e) { return e.expanding_areas && e.expanding_areas.length; });
    if (hasAreas) {
        var tbl = document.getElementById('expansionTable');
        var html = '<thead><tr><th>Month</th><th>New Functions</th><th>Expanding Areas</th></tr></thead><tbody>';
        expansion.forEach(function(e) {
            if (e.new_functions > 0) {
                html += '<tr><td>' + esc(e.month) + '</td><td>' + e.new_functions + '</td><td class="truncate">' + esc((e.expanding_areas || []).join(', ')) + '</td></tr>';
            }
        });
        html += '</tbody>';
        tbl.innerHTML = html;
    }
})();

// Analysis prose rendering
(function() {
    var a = DATA.analysis || {};
    if (!a || Object.keys(a).length === 0) return;

    // Executive Summary
    var exec = a.executive;
    if (exec && exec.headline) {
        document.getElementById('executive').style.display = '';
        document.getElementById('executivePreview').textContent = exec.headline;
        var html = '<div class="analysis-prose">';
        html += '<p><strong>' + esc(exec.headline) + '</strong></p>';
        if (exec.overview) html += '<p>' + esc(exec.overview) + '</p>';
        if (exec.key_findings && exec.key_findings.length) {
            html += '<ul>';
            exec.key_findings.forEach(function(f) { html += '<li>' + esc(f) + '</li>'; });
            html += '</ul>';
        }
        if (exec.risk_assessment) html += '<p><span class="prose-label">Risk Assessment</span>' + esc(exec.risk_assessment) + '</p>';
        html += '</div>';
        document.getElementById('executiveProse').innerHTML = html;
    }

    // Project Arc
    var arc = a.project_arc;
    if (arc && arc.narrative) {
        document.getElementById('project-arc').style.display = '';
        if (arc.arc_type) document.getElementById('arcPreview').textContent = arc.arc_type.charAt(0).toUpperCase() + arc.arc_type.slice(1) + ' arc';
        var html = '<div class="analysis-prose">';
        html += '<span class="prose-label">Project Story</span>';
        html += '<p>' + esc(arc.narrative) + '</p>';
        if (arc.key_moments && arc.key_moments.length) {
            html += '<span class="prose-label">Key Moments</span><ul>';
            arc.key_moments.forEach(function(m) { html += '<li>' + esc(m) + '</li>'; });
            html += '</ul>';
        }
        if (arc.looking_back) html += '<p><em>' + esc(arc.looking_back) + '</em></p>';
        html += '</div>';
        document.getElementById('arcProse').innerHTML = html;
    }

    // Chapter cards
    var chapters = a.period_chapters;
    if (chapters && chapters.chapters && chapters.chapters.length) {
        document.getElementById('project-arc').style.display = '';
        var el = document.getElementById('chapterCards');
        var html = '';
        chapters.chapters.forEach(function(ch) {
            var badgeType = 'steady';
            var period = ch.period || '';
            // Try to match phase type from phases data
            var phases = a.phases || [];
            for (var i = 0; i < phases.length; i++) {
                if (period.indexOf(phases[i].start_month) >= 0) {
                    badgeType = phases[i].phase_type || 'steady';
                    break;
                }
            }
            html += '<div class="chapter-card">';
            html += '<div class="chapter-header">';
            html += '<span class="chapter-period">' + esc(period) + '</span>';
            html += '<span class="chapter-title">' + esc(ch.title || '') + '</span>';
            html += '</div>';
            if (ch.summary) html += '<p class="chapter-summary">' + esc(ch.summary) + '</p>';
            html += '<span class="chapter-badge ' + badgeType + '">' + esc(badgeType) + '</span>';
            html += '</div>';
        });
        el.innerHTML = html;
    }

    // Archaeology
    var arch = a.archaeology;
    if (arch && (arch.fragile_narrative || arch.stale_narrative)) {
        document.getElementById('archaeology').style.display = '';
        var html = '<div class="archaeology-section">';
        // Fragile zones
        if (arch.fragile_narrative) {
            html += '<div class="analysis-prose"><span class="prose-label">Fragile Zones</span>';
            html += '<p>' + esc(arch.fragile_narrative) + '</p></div>';
        }
        if (arch.fragile_items && arch.fragile_items.length) {
            var items = arch.fragile_items;
            html += '<table><thead><tr><th>Function</th><th>File</th><th>Observation</th></tr></thead><tbody>';
            items.forEach(function(item) {
                html += '<tr><td class="mono">' + esc(item.name || '') + '</td><td class="truncate">' + esc(item.file || '') + '</td><td>' + esc(item.observation || '') + '</td></tr>';
            });
            html += '</tbody></table>';
        }
        // Stale zones
        if (arch.stale_narrative) {
            html += '<div class="analysis-prose"><span class="prose-label">Stale Zones</span>';
            html += '<p>' + esc(arch.stale_narrative) + '</p></div>';
        }
        if (arch.stale_items && arch.stale_items.length) {
            var items = arch.stale_items;
            html += '<table><thead><tr><th>Function</th><th>File</th><th>Observation</th></tr></thead><tbody>';
            items.forEach(function(item) {
                html += '<tr><td class="mono">' + esc(item.name || '') + '</td><td class="truncate">' + esc(item.file || '') + '</td><td>' + esc(item.observation || '') + '</td></tr>';
            });
            html += '</tbody></table>';
        }
        html += '</div>';
        document.getElementById('archaeologyContent').innerHTML = html;
    }

    // Timeline Narrative
    var tl = a.timeline;
    if (tl && tl.narrative) {
        var html = '<div class="analysis-prose">';
        html += '<span class="prose-label">Analysis</span>';
        html += '<p>' + esc(tl.narrative) + '</p>';
        if (tl.phases && tl.phases.length) {
            html += '<ul>';
            tl.phases.forEach(function(p) { html += '<li><strong>' + esc(p.period) + ':</strong> ' + esc(p.description) + '</li>'; });
            html += '</ul>';
        }
        if (tl.peak_activity) html += '<p>' + esc(tl.peak_activity) + '</p>';
        html += '</div>';
        document.getElementById('timelineProse').innerHTML = html;
    }

    // Hotspot Analysis
    var hs = a.hotspots;
    if (hs && hs.overview) {
        var html = '<div class="analysis-prose">';
        html += '<span class="prose-label">Analysis</span>';
        html += '<p>' + esc(hs.overview) + '</p>';
        if (hs.hotspots && hs.hotspots.length) {
            html += '<ul>';
            hs.hotspots.forEach(function(h) {
                var riskCls = 'risk-' + (h.risk || 'low');
                html += '<li><strong>' + esc(h.name) + '</strong> <span class="' + riskCls + '">[' + esc(h.risk || 'low') + ']</span> ' + esc(h.explanation) + '</li>';
            });
            html += '</ul>';
        }
        html += '</div>';
        document.getElementById('hotspotsProse').innerHTML = html;
    }

    // Stability Assessment
    var st = a.stability;
    if (st && st.assessment) {
        var html = '<div class="analysis-prose">';
        html += '<span class="prose-label">Analysis</span>';
        html += '<p>' + esc(st.assessment) + '</p>';
        if (st.concerns && st.concerns.length) {
            html += '<ul>';
            st.concerns.forEach(function(c) { html += '<li>' + esc(c) + '</li>'; });
            html += '</ul>';
        }
        if (st.recommendation) html += '<p><strong>Recommendation:</strong> ' + esc(st.recommendation) + '</p>';
        html += '</div>';
        document.getElementById('stabilityProse').innerHTML = html;
    }

    // Contributors + Ownership
    var ct = a.contributors;
    var own = a.ownership;
    if ((ct && ct.overview) || (own && own.narrative)) {
        var html = '<div class="analysis-prose">';
        html += '<span class="prose-label">Analysis</span>';
        if (own && own.narrative) html += '<p>' + esc(own.narrative) + '</p>';
        else if (ct && ct.overview) html += '<p>' + esc(ct.overview) + '</p>';
        if (ct && ct.bus_factor) html += '<p><strong>Bus Factor:</strong> ' + esc(ct.bus_factor) + '</p>';
        if (own && own.convergence_points && own.convergence_points.length) {
            html += '<span class="prose-label">Convergence Points</span><ul>';
            own.convergence_points.forEach(function(p) { html += '<li>' + esc(p) + '</li>'; });
            html += '</ul>';
        }
        if (ct && ct.highlights && ct.highlights.length) {
            html += '<ul>';
            ct.highlights.forEach(function(h) { html += '<li>' + esc(h) + '</li>'; });
            html += '</ul>';
        }
        html += '</div>';
        document.getElementById('contributorsProse').innerHTML = html;
    }
})();
</script>
</body>
</html>'''
