"""LLM-powered historical archaeology analysis for GitAST reports.

Architecture:
  Layer 0: Historical data extraction (Python, no LLM)
  Layer 1: Period classification (1 LLM call)
  Layer 2: Pattern analysis (2 LLM calls)
  Layer 3: Narrative synthesis (2 LLM calls)
  Total: 5 LLM calls with --no-llm fallback for all sections.

Each section has:
- prompt_builder(data) -> str: builds prompt with data + JSON schema
- json_renderer(llm_result, data) -> dict: processes LLM JSON into template data
- fallback(data) -> dict: static summary when LLM unavailable
"""
import json
import os
import re
import statistics
from typing import Any, Dict, List, Optional

from .llm import LLMClient

# Enforce English output from all LLM prompts
LANG_PREAMBLE = """CRITICAL INSTRUCTIONS:
- You MUST respond ONLY in English. Do NOT use Chinese, Japanese, Korean, or any non-English language.
- Do NOT include any thinking, reasoning, or explanation. Output ONLY the raw JSON object.
- All string values in the JSON MUST be in English.

"""

# ---------------------------------------------------------------------------
# Theme classification patterns for commit messages
# ---------------------------------------------------------------------------

THEME_PATTERNS = {
    'fix': re.compile(r'^(?:fix|bug|patch|hotfix|resolve)', re.IGNORECASE),
    'feature': re.compile(r'^(?:add|implement|create|new|introduce|enable)', re.IGNORECASE),
    'refactor': re.compile(r'^(?:refactor|split|modular|reorgan|restructur|consolidat|clean)', re.IGNORECASE),
    'docs': re.compile(r'^(?:doc|readme|changelog|comment|update.*doc)', re.IGNORECASE),
    'test': re.compile(r'^(?:test|spec|assert|coverage)', re.IGNORECASE),
    'config': re.compile(r'^(?:config|setup|deploy|ci|build|version|bump)', re.IGNORECASE),
    'remove': re.compile(r'^(?:remove|delete|deprecate|drop)', re.IGNORECASE),
}


# ===========================================================================
# Layer 0: Historical Data Extraction (all Python, no LLM)
# ===========================================================================

def gather_project_context(repo_path: str) -> Dict:
    """Extract project context: README description, doc inventory, changelog milestones."""
    result: Dict[str, Any] = {
        'description': '',
        'doc_count': 0,
        'doc_names': [],
        'milestones': [],
    }

    # README first paragraph (max 500 chars)
    for readme_name in ('README.md', 'README.rst', 'README.txt', 'README'):
        readme_path = os.path.join(repo_path, readme_name)
        if os.path.isfile(readme_path):
            try:
                with open(readme_path, 'r', encoding='utf-8', errors='replace') as f:
                    text = f.read(2000)
                # Skip title lines (# heading)
                lines = text.split('\n')
                para_lines = []
                past_heading = False
                for line in lines:
                    stripped = line.strip()
                    if not past_heading:
                        if stripped and not stripped.startswith('#') and not stripped.startswith('='):
                            past_heading = True
                            para_lines.append(stripped)
                    elif stripped:
                        para_lines.append(stripped)
                    elif para_lines:
                        break  # end of first paragraph
                result['description'] = ' '.join(para_lines)[:500]
            except OSError:
                pass
            break

    # Doc inventory
    docs_dir = os.path.join(repo_path, 'docs')
    if os.path.isdir(docs_dir):
        doc_files = []
        try:
            for entry in os.listdir(docs_dir):
                if entry.endswith(('.md', '.rst', '.txt')):
                    doc_files.append(entry)
        except OSError:
            pass
        result['doc_count'] = len(doc_files)
        result['doc_names'] = sorted(doc_files)[:10]

    # Changelog milestones
    for cl_name in ('CHANGELOG.md', 'CHANGES.md', 'HISTORY.md', 'changelog.md'):
        cl_path = os.path.join(repo_path, cl_name)
        if os.path.isfile(cl_path):
            try:
                with open(cl_path, 'r', encoding='utf-8', errors='replace') as f:
                    cl_text = f.read(10000)
                result['milestones'] = _extract_milestones(cl_text)
            except OSError:
                pass
            break

    return result


def _extract_milestones(changelog_text: str) -> List[Dict]:
    """Extract version + date pairs from changelog text."""
    # Match patterns like: ## [1.0.0] - 2025-11-16  or  ## v1.0.0 (2025-11-16)
    pattern = re.compile(
        r'##\s+\[?v?(\d+\.\d+(?:\.\d+)?)\]?\s*[-—(]\s*(\d{4}-\d{2}-\d{2})'
    )
    milestones = []
    for match in pattern.finditer(changelog_text):
        version = match.group(1)
        date = match.group(2)
        # Try to get title text after the date on the same line
        line_end = changelog_text.find('\n', match.end())
        rest = changelog_text[match.end():line_end].strip() if line_end > 0 else ''
        rest = rest.lstrip(')').lstrip('—').lstrip('-').strip()
        milestones.append({
            'version': version,
            'date': date,
            'title': rest[:80] if rest else '',
        })
    return milestones[:20]


def detect_project_phases(timeline: List[Dict]) -> List[Dict]:
    """Analyze monthly timeline data to group consecutive months into development phases.

    Args:
        timeline: List of {'month', 'commits', 'changes', 'functions', 'authors'}

    Returns:
        List of phase dicts with start/end months, type, and aggregate stats.
    """
    if not timeline:
        return []

    # Compute median monthly change count
    change_counts = [m.get('changes', 0) for m in timeline]
    if not change_counts:
        return []

    median_changes = statistics.median(change_counts) if change_counts else 1
    if median_changes == 0:
        median_changes = 1  # avoid division by zero

    # Classify each month
    classified = []
    for m in timeline:
        changes = m.get('changes', 0)
        ratio = changes / median_changes
        if ratio > 1.5:
            level = 'high'
        elif ratio >= 0.3:
            level = 'normal'
        else:
            level = 'low'
        classified.append({**m, '_level': level})

    # Group consecutive same-level months
    groups = []
    current_group = [classified[0]]
    for m in classified[1:]:
        if m['_level'] == current_group[-1]['_level']:
            current_group.append(m)
        else:
            groups.append(current_group)
            current_group = [m]
    groups.append(current_group)

    # Label phases based on level and position
    phases = []
    prev_level = None
    for group in groups:
        level = group[0]['_level']
        total_commits = sum(m.get('commits', 0) for m in group)
        total_changes = sum(m.get('changes', 0) for m in group)
        authors_per_month = [m.get('authors', 0) for m in group]
        avg_authors = round(sum(authors_per_month) / len(authors_per_month), 1) if authors_per_month else 0

        # Determine phase type based on transitions
        if level == 'high':
            if prev_level in ('low', None):
                phase_type = 'growth'
            elif prev_level == 'normal':
                phase_type = 'growth'
            else:
                phase_type = 'peak'
        elif level == 'normal':
            if prev_level == 'high':
                phase_type = 'decline'
            else:
                phase_type = 'steady'
        else:  # low
            if prev_level in ('high', 'normal'):
                phase_type = 'decline'
            else:
                phase_type = 'dormant'

        # Check for revival: low followed by this being high and prev was low
        if level == 'high' and prev_level == 'low':
            phase_type = 'revival'

        phases.append({
            'start_month': group[0]['month'],
            'end_month': group[-1]['month'],
            'phase_type': phase_type,
            'months': len(group),
            'total_commits': total_commits,
            'total_changes': total_changes,
            'avg_authors': avg_authors,
        })
        prev_level = level

    return phases


def classify_commit_theme(message: str) -> str:
    """Classify a commit message into a theme category."""
    msg = message.strip()
    for theme, pattern in THEME_PATTERNS.items():
        if pattern.match(msg):
            return theme
    return 'other'


def extract_period_themes(commits_by_month: List[Dict], phases: List[Dict]) -> List[Dict]:
    """Extract themes from commit messages grouped by detected phases.

    Args:
        commits_by_month: from DataStore.get_commits_by_month()
        phases: from detect_project_phases()

    Returns:
        List of theme dicts per phase.
    """
    # Build month → commits lookup
    month_map: Dict[str, list] = {}
    for entry in commits_by_month:
        month_map[entry['month']] = entry['commits']

    # Build month range for each phase
    all_months = sorted(month_map.keys())

    result = []
    for i, phase in enumerate(phases):
        start = phase['start_month']
        end = phase['end_month']

        # Collect commits in this phase's month range
        phase_commits = []
        for m in all_months:
            if start <= m <= end:
                phase_commits.extend(month_map.get(m, []))

        # Classify themes
        themes: Dict[str, int] = {}
        sample_by_theme: Dict[str, list] = {}
        file_counts: Dict[str, int] = {}

        for c in phase_commits:
            theme = classify_commit_theme(c.get('message', ''))
            themes[theme] = themes.get(theme, 0) + 1
            if theme not in sample_by_theme:
                sample_by_theme[theme] = []
            if len(sample_by_theme[theme]) < 3:
                msg = c.get('message', '').split('\n')[0][:120]
                if msg:
                    sample_by_theme[theme].append(msg)

        # Determine dominant theme
        dominant = max(themes, key=themes.get) if themes else 'other'

        # Collect sample messages (top 5 from dominant theme, then others)
        samples = list(sample_by_theme.get(dominant, []))
        for theme, msgs in sample_by_theme.items():
            if theme != dominant:
                for msg in msgs:
                    if len(samples) >= 5:
                        break
                    samples.append(msg)

        result.append({
            'phase_index': i,
            'themes': themes,
            'dominant_theme': dominant,
            'sample_messages': samples[:5],
        })

    return result


def identify_activity_bursts(phases: List[Dict], timeline: List[Dict]) -> List[Dict]:
    """Detect phases with total_changes > 2x the overall average per-month rate."""
    if not timeline or not phases:
        return []

    total_changes = sum(m.get('changes', 0) for m in timeline)
    avg_per_month = total_changes / len(timeline) if timeline else 1

    bursts = []
    for phase in phases:
        per_month = phase['total_changes'] / phase['months'] if phase['months'] else 0
        if per_month > 2 * avg_per_month:
            bursts.append({
                'start_month': phase['start_month'],
                'end_month': phase['end_month'],
                'phase_type': phase['phase_type'],
                'total_changes': phase['total_changes'],
                'intensity': round(per_month / avg_per_month, 1) if avg_per_month else 0,
            })
    return bursts


# ===========================================================================
# Layer 1: Period Classification (1 LLM call)
# ===========================================================================

PERIOD_CHAPTERS_SCHEMA = '''{
  "chapters": [
    {
      "period": "YYYY-MM to YYYY-MM",
      "title": "3-5 word chapter title",
      "summary": "2-3 sentences on what happened in this period",
      "significance": "1 sentence on why this period matters"
    }
  ]
}'''


def period_chapters_prompt(data: Dict) -> str:
    """Build prompt for period classification."""
    phases = data.get('phases', [])
    themes = data.get('period_themes', [])
    milestones = data.get('project_context', {}).get('milestones', [])
    description = data.get('project_context', {}).get('description', '')
    expansion = data.get('feature_expansion', [])

    # Merge themes into phases for the prompt
    phase_info = []
    for i, phase in enumerate(phases):
        info = {**phase}
        if i < len(themes):
            info['themes'] = themes[i].get('themes', {})
            info['dominant_theme'] = themes[i].get('dominant_theme', '')
            info['sample_messages'] = themes[i].get('sample_messages', [])
        # Add expansion data for this period
        period_expansion = [e for e in expansion
                           if phase['start_month'] <= e['month'] <= phase['end_month']]
        if period_expansion:
            info['new_functions'] = sum(e['new_functions'] for e in period_expansion)
            info['expanding_areas'] = list(set(
                a for e in period_expansion for a in e.get('expanding_areas', [])
            ))[:5]
        phase_info.append(info)

    return LANG_PREAMBLE + f"""Analyze the git history of a software project and provide chapter descriptions for each development phase.

Project: {description or 'A software project'}

Detected development phases with commit themes:
{json.dumps(phase_info, default=str, indent=2)}

Milestones from changelog:
{json.dumps(milestones, default=str)}

For each phase, write a chapter title and summary explaining what was happening in the project during that period. Reference specific commit themes and feature additions where relevant.

Respond with JSON matching this schema:
{PERIOD_CHAPTERS_SCHEMA}"""


def period_chapters_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {'chapters': llm_result.get('chapters', [])}


def period_chapters_fallback(data: Dict) -> Dict:
    """Generate chapter titles from dominant themes."""
    phases = data.get('phases', [])
    themes = data.get('period_themes', [])

    theme_labels = {
        'fix': 'Bug Fix Sprint',
        'feature': 'Feature Development',
        'refactor': 'Code Restructuring',
        'docs': 'Documentation Push',
        'test': 'Testing Phase',
        'config': 'Configuration & Setup',
        'remove': 'Cleanup & Removal',
        'other': 'General Development',
    }

    chapters = []
    for i, phase in enumerate(phases):
        dominant = themes[i].get('dominant_theme', 'other') if i < len(themes) else 'other'
        title = theme_labels.get(dominant, 'Development Phase')
        phase_type = phase.get('phase_type', 'steady')

        period = phase['start_month']
        if phase['start_month'] != phase['end_month']:
            period = f"{phase['start_month']} to {phase['end_month']}"

        summary = (f"{phase.get('months', 1)} month(s) of {phase_type} activity "
                   f"with {phase.get('total_commits', 0)} commits and "
                   f"{phase.get('total_changes', 0)} function changes.")
        if i < len(themes) and themes[i].get('sample_messages'):
            summary += f" Dominant activity: {dominant}."

        chapters.append({
            'period': period,
            'title': f"{title} ({phase_type.title()})",
            'summary': summary,
            'significance': f"A {phase_type} phase with {phase.get('total_commits', 0)} commits.",
        })
    return {'chapters': chapters}


# ===========================================================================
# Layer 2: Pattern Analysis (2 LLM calls)
# ===========================================================================

# --- 2a. Archaeology (Fragile & Stale) ---

ARCHAEOLOGY_SCHEMA = '''{
  "fragile_narrative": "2-3 sentences on what the fragile zones reveal",
  "fragile_items": [
    {"name": "func_name", "file": "file_path", "observation": "1 sentence on why this keeps changing"}
  ],
  "stale_narrative": "2-3 sentences on what the stale zones mean",
  "stale_items": [
    {"name": "func_name", "file": "file_path", "observation": "1 sentence on what this forgotten code suggests"}
  ]
}'''


def archaeology_prompt(data: Dict) -> str:
    fragile = data.get('fragile_zones', [])[:10]
    stale = data.get('stale_zones', [])[:10]
    description = data.get('project_context', {}).get('description', '')

    return LANG_PREAMBLE + f"""Analyze these historical patterns in a software project's codebase.

Project: {description or 'A software project'}

FRAGILE ZONES — functions that keep getting reworked:
{json.dumps(fragile, default=str, indent=2)}

STALE ZONES — functions that haven't been touched in a long time:
{json.dumps(stale, default=str, indent=2)}

Explain these patterns: why do the fragile functions keep changing? What do the stale zones suggest about the codebase architecture? Be specific about each function.

Respond with JSON matching this schema:
{ARCHAEOLOGY_SCHEMA}"""


def archaeology_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'fragile_narrative': llm_result.get('fragile_narrative', ''),
        'fragile_items': llm_result.get('fragile_items', []),
        'stale_narrative': llm_result.get('stale_narrative', ''),
        'stale_items': llm_result.get('stale_items', []),
    }


def archaeology_fallback(data: Dict) -> Dict:
    fragile = data.get('fragile_zones', [])
    stale = data.get('stale_zones', [])

    fragile_items = []
    for f in fragile[:10]:
        fragile_items.append({
            'name': f.get('function_name', ''),
            'file': f.get('file_path', ''),
            'observation': f"Modified {f.get('modify_count', 0)} times by {f.get('author_count', 0)} author(s).",
        })

    stale_items = []
    for s in stale[:10]:
        lc = s.get('last_changed')
        if lc:
            stale_items.append({
                'name': s.get('function_name', ''),
                'file': s.get('file_path', ''),
                'observation': f"Last changed {lc.strftime('%Y-%m-%d') if hasattr(lc, 'strftime') else str(lc)}, {s.get('total_changes', 0)} total changes.",
            })
        else:
            stale_items.append({
                'name': s.get('function_name', ''),
                'file': s.get('file_path', ''),
                'observation': 'Never changed since indexing.',
            })

    fragile_narrative = (
        f"{len(fragile)} functions have been modified 5+ times, suggesting ongoing refinement or instability."
        if fragile else "No fragile zones detected — no function has been modified 5+ times."
    )
    stale_narrative = (
        f"{len(stale)} functions identified as stale — untouched for extended periods."
        if stale else "No particularly stale functions detected."
    )

    return {
        'fragile_narrative': fragile_narrative,
        'fragile_items': fragile_items,
        'stale_narrative': stale_narrative,
        'stale_items': stale_items,
    }


# --- 2b. Contributors & Ownership ---

OWNERSHIP_SCHEMA = '''{
  "narrative": "2-3 sentences on how authorship evolved over the project life",
  "convergence_points": ["functions/areas where multiple authors work together"],
  "ownership_shifts": ["notable changes in who works on what over time"]
}'''


def ownership_prompt(data: Dict) -> str:
    authors = data.get('authors', [])[:10]
    coauthorship = data.get('coauthorship_patterns', [])[:15]
    description = data.get('project_context', {}).get('description', '')

    return LANG_PREAMBLE + f"""Analyze contributor dynamics and ownership evolution for this project.

Project: {description or 'A software project'}

Author contributions:
{json.dumps(authors, default=str, indent=2)}

Functions touched by multiple authors (convergence points):
{json.dumps(coauthorship, default=str, indent=2)}

How has ownership evolved? Where do authors converge? Where do they work independently?

Respond with JSON matching this schema:
{OWNERSHIP_SCHEMA}"""


def ownership_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'narrative': llm_result.get('narrative', ''),
        'convergence_points': llm_result.get('convergence_points', []),
        'ownership_shifts': llm_result.get('ownership_shifts', []),
    }


def ownership_fallback(data: Dict) -> Dict:
    authors = data.get('authors', [])
    coauthorship = data.get('coauthorship_patterns', [])

    if not authors:
        return {'narrative': 'No contributor data available.', 'convergence_points': [], 'ownership_shifts': []}

    total_changes = sum(a.get('change_count', 0) for a in authors)
    top = authors[0]
    top_pct = round(top.get('change_count', 0) / total_changes * 100) if total_changes else 0

    narrative = f"{len(authors)} contributors with {total_changes} total changes. "
    narrative += f"{top.get('author', '?')} leads with {top_pct}% of all changes."

    convergence = []
    for c in coauthorship[:5]:
        convergence.append(
            f"{c['function_name']} in {c['file_path']} ({c['author_count']} authors: {', '.join(c.get('authors', []))})"
        )

    return {
        'narrative': narrative,
        'convergence_points': convergence,
        'ownership_shifts': [f"Top contributor owns {top_pct}% of changes"] if top_pct > 50 else [],
    }


# ===========================================================================
# Layer 3: Narrative Synthesis (2 LLM calls)
# ===========================================================================

# --- 3a. Project Arc ---

PROJECT_ARC_SCHEMA = '''{
  "narrative": "4-6 sentences telling the full project story",
  "arc_type": "growth|maturation|revival|expansion|consolidation",
  "key_moments": ["1 sentence per significant turning point"],
  "looking_back": "1 sentence: what does this history reveal about the project?"
}'''


def project_arc_prompt(data: Dict) -> str:
    chapters = data.get('analysis', {}).get('period_chapters', {}).get('chapters', [])
    archaeology = data.get('analysis', {}).get('archaeology', {})
    milestones = data.get('project_context', {}).get('milestones', [])
    description = data.get('project_context', {}).get('description', '')
    stats = data.get('stats', {})
    expansion = data.get('feature_expansion', [])

    # Summarize expansion trajectory
    expansion_summary = ''
    if expansion:
        total_added = sum(e['new_functions'] for e in expansion)
        peak_month = max(expansion, key=lambda e: e['new_functions'])
        expansion_summary = (
            f"Over the project's life, {total_added} functions were added. "
            f"Peak feature building was in {peak_month['month']} with {peak_month['new_functions']} new functions."
        )

    return LANG_PREAMBLE + f"""Weave these development chapters and patterns into a cohesive project story.

Project: {description or 'A software project'}
Stats: {json.dumps(stats, default=str)}

Period chapters:
{json.dumps(chapters, default=str, indent=2)}

Feature expansion: {expansion_summary}

Archaeology findings:
- Fragile zones: {archaeology.get('fragile_narrative', 'None detected')}
- Stale zones: {archaeology.get('stale_narrative', 'None detected')}

Milestones:
{json.dumps(milestones, default=str)}

Tell the full story of this project's evolution. What is the overall arc? What were the turning points?

Respond with JSON matching this schema:
{PROJECT_ARC_SCHEMA}"""


def project_arc_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'narrative': llm_result.get('narrative', ''),
        'arc_type': llm_result.get('arc_type', ''),
        'key_moments': llm_result.get('key_moments', []),
        'looking_back': llm_result.get('looking_back', ''),
    }


def project_arc_fallback(data: Dict) -> Dict:
    chapters = data.get('analysis', {}).get('period_chapters', {}).get('chapters', [])
    phases = data.get('phases', [])
    stats = data.get('stats', {})

    # Concatenate chapter summaries
    narrative_parts = []
    for ch in chapters:
        narrative_parts.append(ch.get('summary', ''))
    narrative = ' '.join(narrative_parts) if narrative_parts else (
        f"Project contains {stats.get('functions', 0)} functions "
        f"across {stats.get('commits', 0)} commits."
    )

    # Determine arc type from phase types
    phase_types = [p['phase_type'] for p in phases] if phases else []
    if 'growth' in phase_types and 'peak' in phase_types:
        arc_type = 'growth'
    elif 'revival' in phase_types:
        arc_type = 'revival'
    elif 'decline' in phase_types:
        arc_type = 'maturation'
    else:
        arc_type = 'expansion'

    key_moments = []
    for ch in chapters[:5]:
        if ch.get('significance'):
            key_moments.append(ch['significance'])

    return {
        'narrative': narrative,
        'arc_type': arc_type,
        'key_moments': key_moments,
        'looking_back': f"A project with {len(phases)} distinct development phases." if phases else '',
    }


# --- 3b. Executive Summary (enhanced) ---

EXECUTIVE_SCHEMA = '''{
  "headline": "One sentence project characterization",
  "overview": "2-3 sentences on project history and trajectory",
  "key_findings": ["finding with specific number", "..."],
  "risk_assessment": "1-2 sentences on code health risks from a historical perspective"
}'''


def executive_prompt(data: Dict) -> str:
    stats = data.get('stats', {})
    description = data.get('project_context', {}).get('description', '')
    arc = data.get('analysis', {}).get('project_arc', {})
    archaeology = data.get('analysis', {}).get('archaeology', {})
    milestones = data.get('project_context', {}).get('milestones', [])
    authors = data.get('authors', [])
    stability_dist = data.get('stability_dist', {})

    return LANG_PREAMBLE + f"""Write an executive summary for this git repository's archaeological analysis.

Project: {description or 'A software project'}

Statistics:
- {stats.get('functions', 0)} functions tracked
- {stats.get('commits', 0)} commits indexed
- {stats.get('changes', 0)} function changes recorded
- {len(authors)} contributors
- Stability: {stability_dist.get('stable', 0)} stable, {stability_dist.get('moderate', 0)} moderate, {stability_dist.get('volatile', 0)} volatile, {stability_dist.get('critical', 0)} critical

Project story: {arc.get('narrative', 'No arc data available.')}
Arc type: {arc.get('arc_type', 'unknown')}

Fragile zones: {archaeology.get('fragile_narrative', 'None detected')}
Stale zones: {archaeology.get('stale_narrative', 'None detected')}

Milestones: {json.dumps(milestones[:5], default=str)}
Top contributors: {json.dumps(authors[:5], default=str)}

Respond with JSON matching this schema:
{EXECUTIVE_SCHEMA}"""


def executive_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'headline': llm_result.get('headline', ''),
        'overview': llm_result.get('overview', ''),
        'key_findings': llm_result.get('key_findings', []),
        'risk_assessment': llm_result.get('risk_assessment', ''),
    }


def executive_fallback(data: Dict) -> Dict:
    stats = data.get('stats', {})
    dist = data.get('stability_dist', {})
    authors = data.get('authors', [])
    phases = data.get('phases', [])
    fragile = data.get('fragile_zones', [])
    stale = data.get('stale_zones', [])

    n_func = stats.get('functions', 0)
    n_commits = stats.get('commits', 0)
    n_changes = stats.get('changes', 0)
    n_critical = dist.get('critical', 0)
    n_volatile = dist.get('volatile', 0)

    findings = [
        f"{n_func} functions tracked across {n_commits} commits",
        f"{n_changes} function-level changes recorded",
    ]
    if phases:
        findings.append(f"{len(phases)} distinct development phases detected")
    if fragile:
        findings.append(f"{len(fragile)} fragile zones (functions modified 5+ times)")
    if n_critical + n_volatile > 0:
        findings.append(f"{n_critical + n_volatile} functions flagged as volatile or critical")
    if authors:
        findings.append(f"{len(authors)} contributors, top: {authors[0].get('author', 'unknown')}")

    return {
        'headline': f"Repository with {n_func} functions across {n_commits} commits",
        'overview': (f"This repository contains {n_func} tracked functions with "
                     f"{n_changes} recorded changes from {len(authors)} contributors. "
                     f"{len(phases)} development phases detected."),
        'key_findings': findings,
        'risk_assessment': (
            f"{n_critical} critical and {n_volatile} volatile functions identified."
            if n_critical + n_volatile > 0
            else "No critical stability concerns detected."
        ),
    }


# ===========================================================================
# Kept for backward compatibility — old section definitions (hotspots, etc.)
# ===========================================================================

HOTSPOT_SCHEMA = '''{
  "overview": "1-2 sentences on hotspot patterns",
  "hotspots": [
    {"name": "func_name", "explanation": "Why it changes frequently (1-2 sentences)", "risk": "low|medium|high"}
  ]
}'''


def hotspot_prompt(data: Dict) -> str:
    hotspots = data.get('hotspots', [])[:10]
    return LANG_PREAMBLE + f"""Analyze these code hotspots — the most frequently changed functions in a git repository.
For each, explain WHY it likely changes so often and assess risk.

Hotspot data:
{json.dumps(hotspots, default=str)}

Respond with JSON matching this schema:
{HOTSPOT_SCHEMA}"""


def hotspot_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'overview': llm_result.get('overview', ''),
        'hotspots': llm_result.get('hotspots', []),
    }


def hotspot_fallback(data: Dict) -> Dict:
    hotspots = data.get('hotspots', [])[:10]
    items = []
    for h in hotspots:
        name = h.get('function_name', '')
        changes = h.get('change_count', 0)
        authors = h.get('author_count', 0)
        risk = 'high' if changes > 15 else ('medium' if changes > 5 else 'low')
        items.append({
            'name': name,
            'explanation': f"Changed {changes} times by {authors} author(s).",
            'risk': risk,
        })
    return {
        'overview': f"{len(hotspots)} hotspots identified based on change frequency." if hotspots else "No hotspots found.",
        'hotspots': items,
    }


STABILITY_SCHEMA = '''{
  "assessment": "2-3 sentences on overall code stability",
  "concerns": ["concern with number", "..."],
  "recommendation": "1-2 sentences on what to stabilize first"
}'''


def stability_prompt(data: Dict) -> str:
    dist = data.get('stability_dist', {})
    volatile = [s for s in data.get('stability', []) if s.get('rating') in ('volatile', 'critical')][:10]
    return LANG_PREAMBLE + f"""Analyze code stability for this repository.

Distribution: {json.dumps(dist)}
Top volatile functions:
{json.dumps(volatile, default=str)}

Respond with JSON matching this schema:
{STABILITY_SCHEMA}"""


def stability_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'assessment': llm_result.get('assessment', ''),
        'concerns': llm_result.get('concerns', []),
        'recommendation': llm_result.get('recommendation', ''),
    }


def stability_fallback(data: Dict) -> Dict:
    dist = data.get('stability_dist', {})
    total = sum(dist.values())
    concerns = []
    if dist.get('critical', 0) > 0:
        concerns.append(f"{dist['critical']} functions rated critical")
    if dist.get('volatile', 0) > 0:
        concerns.append(f"{dist['volatile']} functions rated volatile")

    stable_pct = round(dist.get('stable', 0) / total * 100) if total > 0 else 0
    return {
        'assessment': f"{stable_pct}% of {total} functions are stable." if total > 0 else "No stability data available.",
        'concerns': concerns if concerns else ["No critical concerns"],
        'recommendation': "Focus on stabilizing critical and volatile functions first." if concerns else "Codebase stability is good.",
    }


TIMELINE_SCHEMA = '''{
  "narrative": "3-4 sentences on project evolution",
  "phases": [{"period": "YYYY-MM to YYYY-MM", "description": "1 sentence"}],
  "peak_activity": "1 sentence about busiest period"
}'''


def timeline_prompt(data: Dict) -> str:
    timeline = data.get('timeline', [])
    return LANG_PREAMBLE + f"""Analyze this project's monthly activity timeline and narrate its evolution.

Monthly data (commits, function changes, authors):
{json.dumps(timeline, default=str)}

Respond with JSON matching this schema:
{TIMELINE_SCHEMA}"""


def timeline_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'narrative': llm_result.get('narrative', ''),
        'phases': llm_result.get('phases', []),
        'peak_activity': llm_result.get('peak_activity', ''),
    }


def timeline_fallback(data: Dict) -> Dict:
    timeline = data.get('timeline', [])
    if not timeline:
        return {'narrative': 'No timeline data available.', 'phases': [], 'peak_activity': ''}

    total_commits = sum(m.get('commits', 0) for m in timeline)
    total_changes = sum(m.get('changes', 0) for m in timeline)
    peak = max(timeline, key=lambda m: m.get('changes', 0))

    return {
        'narrative': f"Project spans {len(timeline)} months with {total_commits} commits and {total_changes} function changes.",
        'phases': [{'period': f"{timeline[0].get('month', '?')} to {timeline[-1].get('month', '?')}", 'description': f"{len(timeline)} months of tracked activity"}],
        'peak_activity': f"Peak activity in {peak.get('month', '?')} with {peak.get('changes', 0)} changes and {peak.get('commits', 0)} commits.",
    }


CONTRIBUTORS_SCHEMA = '''{
  "overview": "1-2 sentences on team dynamics",
  "bus_factor": "1 sentence on knowledge concentration risk",
  "highlights": ["highlight about specific author with numbers"]
}'''


def contributors_prompt(data: Dict) -> str:
    authors = data.get('authors', [])
    return LANG_PREAMBLE + f"""Analyze the contributor dynamics for this repository.

Author data (changes, functions touched, files touched, first commit):
{json.dumps(authors, default=str)}

Respond with JSON matching this schema:
{CONTRIBUTORS_SCHEMA}"""


def contributors_renderer(llm_result: Dict, data: Dict) -> Dict:
    return {
        'overview': llm_result.get('overview', ''),
        'bus_factor': llm_result.get('bus_factor', ''),
        'highlights': llm_result.get('highlights', []),
    }


def contributors_fallback(data: Dict) -> Dict:
    authors = data.get('authors', [])
    if not authors:
        return {'overview': 'No contributor data.', 'bus_factor': '', 'highlights': []}

    total_changes = sum(a.get('change_count', 0) for a in authors)
    top = authors[0]
    top_pct = round(top.get('change_count', 0) / total_changes * 100) if total_changes > 0 else 0

    highlights = [f"{top.get('author', '?')} leads with {top.get('change_count', 0)} changes ({top_pct}% of total)"]
    if len(authors) > 1:
        highlights.append(f"{len(authors)} total contributors")

    bus_factor = f"Top contributor owns {top_pct}% of all changes." if top_pct > 50 else f"Changes distributed across {len(authors)} contributors."

    return {
        'overview': f"{len(authors)} contributors with {total_changes} total changes.",
        'bus_factor': bus_factor,
        'highlights': highlights,
    }


# ===========================================================================
# Section registry (same LLM call pattern for hotspots, stability, timeline)
# ===========================================================================

SECTIONS = [
    ('hotspots', hotspot_prompt, hotspot_renderer, hotspot_fallback, HOTSPOT_SCHEMA),
    ('stability', stability_prompt, stability_renderer, stability_fallback, STABILITY_SCHEMA),
    ('timeline', timeline_prompt, timeline_renderer, timeline_fallback, TIMELINE_SCHEMA),
    ('contributors', contributors_prompt, contributors_renderer, contributors_fallback, CONTRIBUTORS_SCHEMA),
]


# ===========================================================================
# Main analysis runner — layered architecture
# ===========================================================================

def _run_section(name, prompt_fn, renderer_fn, fallback_fn, schema, data, client):
    """Run a single section with LLM or fallback."""
    if client is not None:
        try:
            prompt = prompt_fn(data)
            result = client.complete(prompt, schema_hint=schema)
            if result is not None:
                return renderer_fn(result, data)
            else:
                import sys
                print(f"  [warn] LLM returned None for '{name}', using fallback", file=sys.stderr)
        except Exception as e:
            import sys
            print(f"  [warn] LLM failed for '{name}': {e}, using fallback", file=sys.stderr)
    return fallback_fn(data)


def run_analysis(data: Dict, client: Optional[LLMClient] = None,
                 repo_path: Optional[str] = None) -> Dict:
    """Run layered historical analysis.

    Args:
        data: Prepared report data (from _prepare_data)
        client: LLMClient instance (None = fallback only)
        repo_path: Path to repo for context gathering

    Returns:
        Dict with keys: executive, period_chapters, archaeology, ownership,
        project_arc, hotspots, stability, timeline, contributors,
        plus layer0 data (phases, feature_expansion, etc.)
    """
    analysis: Dict[str, Any] = {}

    # -----------------------------------------------------------------------
    # Layer 0: Historical data extraction (all Python)
    # -----------------------------------------------------------------------
    if repo_path:
        data['project_context'] = gather_project_context(repo_path)
    else:
        data.setdefault('project_context', {
            'description': '', 'doc_count': 0, 'doc_names': [], 'milestones': [],
        })

    # Phase detection from timeline
    phases = detect_project_phases(data.get('timeline', []))
    data['phases'] = phases

    # Commit themes per period (needs store access — use commits_by_month from data)
    commits_by_month = data.get('commits_by_month', [])
    period_themes = extract_period_themes(commits_by_month, phases)
    data['period_themes'] = period_themes

    # Activity bursts
    data['activity_bursts'] = identify_activity_bursts(phases, data.get('timeline', []))

    # Store layer0 results in analysis for report rendering
    analysis['phases'] = phases
    analysis['period_themes'] = period_themes
    analysis['activity_bursts'] = data['activity_bursts']
    analysis['project_context'] = data.get('project_context', {})
    analysis['feature_expansion'] = data.get('feature_expansion', [])
    analysis['fragile_zones'] = data.get('fragile_zones', [])
    analysis['stale_zones'] = data.get('stale_zones', [])

    # -----------------------------------------------------------------------
    # Layer 1: Period classification (1 LLM call)
    # -----------------------------------------------------------------------
    analysis['period_chapters'] = _run_section(
        'period_chapters', period_chapters_prompt, period_chapters_renderer,
        period_chapters_fallback, PERIOD_CHAPTERS_SCHEMA, data, client
    )

    # Make chapters available for Layer 3
    data['analysis'] = analysis

    # -----------------------------------------------------------------------
    # Layer 2: Pattern analysis (2 LLM calls)
    # -----------------------------------------------------------------------
    analysis['archaeology'] = _run_section(
        'archaeology', archaeology_prompt, archaeology_renderer,
        archaeology_fallback, ARCHAEOLOGY_SCHEMA, data, client
    )
    data['analysis'] = analysis  # update for ownership to see archaeology

    analysis['ownership'] = _run_section(
        'ownership', ownership_prompt, ownership_renderer,
        ownership_fallback, OWNERSHIP_SCHEMA, data, client
    )
    data['analysis'] = analysis

    # -----------------------------------------------------------------------
    # Layer 3: Narrative synthesis (2 LLM calls)
    # -----------------------------------------------------------------------
    analysis['project_arc'] = _run_section(
        'project_arc', project_arc_prompt, project_arc_renderer,
        project_arc_fallback, PROJECT_ARC_SCHEMA, data, client
    )
    data['analysis'] = analysis

    analysis['executive'] = _run_section(
        'executive', executive_prompt, executive_renderer,
        executive_fallback, EXECUTIVE_SCHEMA, data, client
    )

    # -----------------------------------------------------------------------
    # Existing sections (hotspots, stability, timeline, contributors)
    # -----------------------------------------------------------------------
    for name, prompt_fn, renderer_fn, fallback_fn, schema in SECTIONS:
        analysis[name] = _run_section(
            name, prompt_fn, renderer_fn, fallback_fn, schema, data, client
        )

    return analysis
