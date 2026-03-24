"""Tests for GitAST v0.3.3 layered historical analysis."""
import os
import tempfile
import shutil

from gitast.analyze import (
    # Layer 0
    gather_project_context, _extract_milestones,
    detect_project_phases, classify_commit_theme,
    extract_period_themes, identify_activity_bursts,
    # Layer 1
    period_chapters_prompt, period_chapters_renderer, period_chapters_fallback,
    # Layer 2
    archaeology_prompt, archaeology_renderer, archaeology_fallback,
    ownership_prompt, ownership_renderer, ownership_fallback,
    # Layer 3
    project_arc_prompt, project_arc_renderer, project_arc_fallback,
    executive_prompt, executive_renderer, executive_fallback,
    # Kept sections
    hotspot_prompt, hotspot_renderer, hotspot_fallback,
    timeline_prompt, timeline_renderer, timeline_fallback,
    stability_prompt, stability_renderer, stability_fallback,
    contributors_prompt, contributors_renderer, contributors_fallback,
    # Runner
    run_analysis, SECTIONS,
)


def _sample_data():
    return {
        'stats': {'functions': 100, 'commits': 50, 'changes': 200, 'blame_entries': 150},
        'timeline': [
            {'month': '2025-01', 'commits': 10, 'changes': 20, 'functions': 15, 'authors': 3},
            {'month': '2025-02', 'commits': 8, 'changes': 15, 'functions': 10, 'authors': 2},
            {'month': '2025-03', 'commits': 15, 'changes': 40, 'functions': 20, 'authors': 4},
        ],
        'hotspots': [
            {'function_name': 'parse', 'file_path': 'src/parser.py', 'change_count': 15, 'author_count': 3},
            {'function_name': 'render', 'file_path': 'src/render.py', 'change_count': 10, 'author_count': 2},
            {'function_name': 'init', 'file_path': 'src/core.py', 'change_count': 3, 'author_count': 1},
        ],
        'stability': [
            {'function_name': 'stable_fn', 'file_path': 'a.py', 'stability_score': 0.95, 'rating': 'stable', 'change_count': 1},
            {'function_name': 'volatile_fn', 'file_path': 'b.py', 'stability_score': 0.2, 'rating': 'critical', 'change_count': 20},
            {'function_name': 'mod_fn', 'file_path': 'c.py', 'stability_score': 0.6, 'rating': 'volatile', 'change_count': 8},
        ],
        'stability_dist': {'stable': 1, 'moderate': 0, 'volatile': 1, 'critical': 1},
        'authors': [
            {'author': 'Alice', 'change_count': 120, 'functions_touched': 50, 'files_touched': 20, 'first_commit': '2025-01-01'},
            {'author': 'Bob', 'change_count': 80, 'functions_touched': 30, 'files_touched': 15, 'first_commit': '2025-02-01'},
        ],
        'languages': [
            {'language': 'python', 'count': 80},
            {'language': 'javascript', 'count': 20},
        ],
        'commits_by_month': [
            {'month': '2025-01', 'commits': [
                {'message': 'Add new parser', 'author': 'Alice', 'hash': 'aaa'},
                {'message': 'Fix timeout bug', 'author': 'Bob', 'hash': 'bbb'},
            ]},
            {'month': '2025-02', 'commits': [
                {'message': 'Refactor core module', 'author': 'Alice', 'hash': 'ccc'},
            ]},
            {'month': '2025-03', 'commits': [
                {'message': 'Add search feature', 'author': 'Alice', 'hash': 'ddd'},
                {'message': 'Implement caching', 'author': 'Bob', 'hash': 'eee'},
            ]},
        ],
        'fragile_zones': [
            {'function_name': 'process', 'file_path': 'core.py', 'change_count': 12,
             'author_count': 3, 'modify_count': 9},
        ],
        'stale_zones': [
            {'function_name': 'legacy_init', 'file_path': 'old.py', 'kind': 'function',
             'language': 'python', 'last_changed': None, 'total_changes': 0},
        ],
        'coauthorship_patterns': [
            {'function_name': 'process', 'file_path': 'core.py',
             'author_count': 3, 'authors': ['Alice', 'Bob', 'Eve'], 'change_count': 12},
        ],
        'feature_expansion': [
            {'month': '2025-01', 'new_functions': 20, 'new_files': ['src/parser.py'],
             'expanding_areas': ['src/'], 'cumulative_functions': 20},
            {'month': '2025-03', 'new_functions': 15, 'new_files': ['src/search.py'],
             'expanding_areas': ['src/'], 'cumulative_functions': 35},
        ],
    }


# ===========================================================================
# Layer 0: Historical Data Extraction
# ===========================================================================

class TestGatherProjectContext:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.tmp)

    def test_no_readme(self):
        result = gather_project_context(self.tmp)
        assert result['description'] == ''
        assert result['doc_count'] == 0
        assert result['milestones'] == []

    def test_with_readme(self):
        with open(os.path.join(self.tmp, 'README.md'), 'w') as f:
            f.write('# My Project\n\nThis is a great project for doing things.\n\nMore text here.\n')
        result = gather_project_context(self.tmp)
        assert 'great project' in result['description']

    def test_with_docs(self):
        docs = os.path.join(self.tmp, 'docs')
        os.makedirs(docs)
        with open(os.path.join(docs, 'guide.md'), 'w') as f:
            f.write('# Guide\n')
        with open(os.path.join(docs, 'api.md'), 'w') as f:
            f.write('# API\n')
        result = gather_project_context(self.tmp)
        assert result['doc_count'] == 2
        assert 'guide.md' in result['doc_names']

    def test_with_changelog(self):
        with open(os.path.join(self.tmp, 'CHANGELOG.md'), 'w') as f:
            f.write('## [1.0.0] - 2025-11-16 First Release\n\n## [0.9.0] - 2025-10-01\n')
        result = gather_project_context(self.tmp)
        assert len(result['milestones']) == 2
        assert result['milestones'][0]['version'] == '1.0.0'
        assert result['milestones'][0]['date'] == '2025-11-16'


class TestExtractMilestones:
    def test_bracket_format(self):
        text = '## [1.0.0] - 2025-11-16 First Release\n## [0.9.0] - 2025-10-01\n'
        ms = _extract_milestones(text)
        assert len(ms) == 2
        assert ms[0]['version'] == '1.0.0'

    def test_v_prefix(self):
        text = '## v2.1.0 (2026-01-15)\n'
        ms = _extract_milestones(text)
        assert len(ms) == 1
        assert ms[0]['version'] == '2.1.0'
        assert ms[0]['date'] == '2026-01-15'

    def test_no_milestones(self):
        ms = _extract_milestones('Just some text without versions')
        assert ms == []


class TestDetectProjectPhases:
    def test_empty_timeline(self):
        assert detect_project_phases([]) == []

    def test_single_month(self):
        phases = detect_project_phases([
            {'month': '2025-01', 'commits': 10, 'changes': 20, 'authors': 2}
        ])
        assert len(phases) == 1
        assert phases[0]['start_month'] == '2025-01'

    def test_multiple_phases(self):
        timeline = [
            {'month': '2025-01', 'commits': 5, 'changes': 10, 'authors': 1},
            {'month': '2025-02', 'commits': 5, 'changes': 10, 'authors': 1},
            {'month': '2025-03', 'commits': 50, 'changes': 100, 'authors': 4},
            {'month': '2025-04', 'commits': 50, 'changes': 100, 'authors': 4},
            {'month': '2025-05', 'commits': 2, 'changes': 3, 'authors': 1},
        ]
        phases = detect_project_phases(timeline)
        assert len(phases) >= 2
        # Should detect the spike in march-april
        phase_types = [p['phase_type'] for p in phases]
        assert any(t in ('growth', 'revival', 'peak') for t in phase_types)

    def test_all_zero_changes(self):
        timeline = [
            {'month': '2025-01', 'commits': 5, 'changes': 0, 'authors': 1},
            {'month': '2025-02', 'commits': 3, 'changes': 0, 'authors': 1},
        ]
        phases = detect_project_phases(timeline)
        assert len(phases) >= 1

    def test_phase_has_required_keys(self):
        phases = detect_project_phases([
            {'month': '2025-01', 'commits': 10, 'changes': 20, 'authors': 2},
            {'month': '2025-02', 'commits': 8, 'changes': 15, 'authors': 2},
        ])
        for p in phases:
            assert 'start_month' in p
            assert 'end_month' in p
            assert 'phase_type' in p
            assert 'months' in p
            assert 'total_commits' in p
            assert 'total_changes' in p
            assert 'avg_authors' in p


class TestClassifyCommitTheme:
    def test_fix(self):
        assert classify_commit_theme('Fix authentication bug') == 'fix'
        assert classify_commit_theme('Bug in parser') == 'fix'

    def test_feature(self):
        assert classify_commit_theme('Add new search feature') == 'feature'
        assert classify_commit_theme('Implement caching') == 'feature'

    def test_refactor(self):
        assert classify_commit_theme('Refactor core module') == 'refactor'

    def test_docs(self):
        assert classify_commit_theme('Document API endpoints') == 'docs'

    def test_other(self):
        assert classify_commit_theme('Update dependencies') == 'other'
        assert classify_commit_theme('') == 'other'


class TestExtractPeriodThemes:
    def test_with_phases(self):
        data = _sample_data()
        phases = detect_project_phases(data['timeline'])
        themes = extract_period_themes(data['commits_by_month'], phases)
        assert len(themes) == len(phases)
        for t in themes:
            assert 'themes' in t
            assert 'dominant_theme' in t
            assert 'sample_messages' in t

    def test_empty_phases(self):
        assert extract_period_themes([], []) == []


class TestIdentifyActivityBursts:
    def test_no_bursts(self):
        timeline = [{'changes': 10}, {'changes': 10}, {'changes': 10}]
        phases = [{'start_month': '2025-01', 'end_month': '2025-03',
                   'total_changes': 30, 'months': 3, 'phase_type': 'steady'}]
        bursts = identify_activity_bursts(phases, timeline)
        assert bursts == []

    def test_with_burst(self):
        timeline = [{'changes': 10}, {'changes': 10}, {'changes': 100}]
        phases = [
            {'start_month': '2025-01', 'end_month': '2025-02',
             'total_changes': 20, 'months': 2, 'phase_type': 'steady'},
            {'start_month': '2025-03', 'end_month': '2025-03',
             'total_changes': 100, 'months': 1, 'phase_type': 'growth'},
        ]
        bursts = identify_activity_bursts(phases, timeline)
        assert len(bursts) >= 1


# ===========================================================================
# Layer 1: Period Classification
# ===========================================================================

class TestPeriodChapters:
    def test_prompt_contains_phases(self):
        data = _sample_data()
        data['phases'] = detect_project_phases(data['timeline'])
        data['period_themes'] = extract_period_themes(data['commits_by_month'], data['phases'])
        data['project_context'] = {'description': 'Test project', 'milestones': []}
        prompt = period_chapters_prompt(data)
        assert 'Test project' in prompt
        assert '2025-' in prompt

    def test_renderer(self):
        llm_result = {
            'chapters': [{'period': '2025-01', 'title': 'Start', 'summary': 'Begin', 'significance': 'First'}]
        }
        result = period_chapters_renderer(llm_result, {})
        assert len(result['chapters']) == 1

    def test_fallback(self):
        data = _sample_data()
        data['phases'] = detect_project_phases(data['timeline'])
        data['period_themes'] = extract_period_themes(data['commits_by_month'], data['phases'])
        result = period_chapters_fallback(data)
        assert 'chapters' in result
        assert len(result['chapters']) == len(data['phases'])

    def test_fallback_empty(self):
        result = period_chapters_fallback({'phases': [], 'period_themes': []})
        assert result['chapters'] == []


# ===========================================================================
# Layer 2: Pattern Analysis
# ===========================================================================

class TestArchaeology:
    def test_prompt_contains_zones(self):
        data = _sample_data()
        data['project_context'] = {'description': 'Test'}
        prompt = archaeology_prompt(data)
        assert 'process' in prompt  # fragile function name
        assert 'legacy_init' in prompt  # stale function name

    def test_renderer(self):
        llm_result = {
            'fragile_narrative': 'Lots of churn.',
            'fragile_items': [{'name': 'f', 'file': 'a.py', 'observation': 'Always changing'}],
            'stale_narrative': 'Forgotten code.',
            'stale_items': [{'name': 'g', 'file': 'b.py', 'observation': 'Never touched'}],
        }
        result = archaeology_renderer(llm_result, {})
        assert result['fragile_narrative'] == 'Lots of churn.'

    def test_fallback(self):
        result = archaeology_fallback(_sample_data())
        assert 'fragile_narrative' in result
        assert 'stale_narrative' in result
        assert len(result['fragile_items']) >= 1
        assert len(result['stale_items']) >= 1

    def test_fallback_empty(self):
        result = archaeology_fallback({'fragile_zones': [], 'stale_zones': []})
        assert 'No fragile' in result['fragile_narrative']


class TestOwnership:
    def test_prompt_contains_authors(self):
        data = _sample_data()
        data['project_context'] = {'description': ''}
        prompt = ownership_prompt(data)
        assert 'Alice' in prompt

    def test_renderer(self):
        llm_result = {
            'narrative': 'Alice dominates.',
            'convergence_points': ['core.py'],
            'ownership_shifts': ['Alice to Bob'],
        }
        result = ownership_renderer(llm_result, {})
        assert result['narrative'] == 'Alice dominates.'

    def test_fallback(self):
        result = ownership_fallback(_sample_data())
        assert 'Alice' in result['narrative']
        assert len(result['convergence_points']) >= 1

    def test_fallback_empty(self):
        result = ownership_fallback({'authors': [], 'coauthorship_patterns': []})
        assert 'No contributor' in result['narrative']


# ===========================================================================
# Layer 3: Narrative Synthesis
# ===========================================================================

class TestProjectArc:
    def test_prompt_contains_story(self):
        data = _sample_data()
        data['phases'] = detect_project_phases(data['timeline'])
        data['analysis'] = {
            'period_chapters': {'chapters': [{'summary': 'The beginning'}]},
            'archaeology': {'fragile_narrative': 'Some fragile code'},
        }
        data['project_context'] = {'description': 'Test', 'milestones': []}
        prompt = project_arc_prompt(data)
        assert 'Test' in prompt

    def test_renderer(self):
        llm_result = {
            'narrative': 'A story.',
            'arc_type': 'growth',
            'key_moments': ['start'],
            'looking_back': 'Good project.',
        }
        result = project_arc_renderer(llm_result, {})
        assert result['arc_type'] == 'growth'

    def test_fallback(self):
        data = _sample_data()
        data['phases'] = detect_project_phases(data['timeline'])
        data['analysis'] = {'period_chapters': {'chapters': [{'summary': 'A period', 'significance': 'Important'}]}}
        result = project_arc_fallback(data)
        assert result['narrative']
        assert result['arc_type'] in ('growth', 'maturation', 'revival', 'expansion', 'consolidation')


class TestExecutive:
    def test_prompt_contains_stats(self):
        data = _sample_data()
        data['project_context'] = {'description': '', 'milestones': []}
        data['analysis'] = {'project_arc': {'narrative': 'Story'}, 'archaeology': {}}
        prompt = executive_prompt(data)
        assert '100 functions' in prompt
        assert '50 commits' in prompt

    def test_renderer(self):
        llm_result = {
            'headline': 'Test headline',
            'overview': 'Test overview',
            'key_findings': ['finding1'],
            'risk_assessment': 'Low risk',
        }
        result = executive_renderer(llm_result, {})
        assert result['headline'] == 'Test headline'
        assert result['key_findings'] == ['finding1']

    def test_fallback(self):
        data = _sample_data()
        data['phases'] = [{'phase_type': 'growth'}]
        result = executive_fallback(data)
        assert 'headline' in result
        assert 'overview' in result
        assert 'key_findings' in result
        assert 'risk_assessment' in result
        assert '100' in result['headline']
        assert len(result['key_findings']) >= 2

    def test_fallback_empty_data(self):
        data = {'stats': {}, 'stability_dist': {}, 'authors': [], 'phases': [],
                'fragile_zones': [], 'stale_zones': []}
        result = executive_fallback(data)
        assert result['headline']


# ===========================================================================
# Kept sections (backward compatibility)
# ===========================================================================

class TestHotspot:
    def test_prompt_contains_hotspots(self):
        prompt = hotspot_prompt(_sample_data())
        assert 'parse' in prompt
        assert 'render' in prompt

    def test_renderer(self):
        llm_result = {
            'overview': 'Test overview',
            'hotspots': [{'name': 'parse', 'explanation': 'Reason', 'risk': 'high'}],
        }
        result = hotspot_renderer(llm_result, {})
        assert result['overview'] == 'Test overview'

    def test_fallback(self):
        result = hotspot_fallback(_sample_data())
        assert result['overview']
        assert len(result['hotspots']) == 3
        assert result['hotspots'][0]['risk'] == 'medium'  # 15 changes (>15 = high)
        assert result['hotspots'][2]['risk'] == 'low'  # 3 changes

    def test_fallback_empty(self):
        result = hotspot_fallback({'hotspots': []})
        assert 'No hotspots' in result['overview']


class TestTimeline:
    def test_prompt_contains_months(self):
        prompt = timeline_prompt(_sample_data())
        assert '2025-01' in prompt

    def test_renderer(self):
        llm_result = {
            'narrative': 'Project grew.',
            'phases': [{'period': '2025-01 to 2025-03', 'description': 'Growth'}],
            'peak_activity': 'March was busiest.',
        }
        result = timeline_renderer(llm_result, {})
        assert result['narrative'] == 'Project grew.'

    def test_fallback(self):
        result = timeline_fallback(_sample_data())
        assert 'narrative' in result
        assert '3 months' in result['narrative']
        assert result['peak_activity']
        assert '2025-03' in result['peak_activity']

    def test_fallback_empty(self):
        result = timeline_fallback({'timeline': []})
        assert 'No timeline' in result['narrative']


class TestStability:
    def test_prompt_contains_dist(self):
        prompt = stability_prompt(_sample_data())
        assert 'stable' in prompt
        assert 'critical' in prompt

    def test_renderer(self):
        llm_result = {
            'assessment': 'Mixed stability.',
            'concerns': ['2 volatile'],
            'recommendation': 'Fix critical first.',
        }
        result = stability_renderer(llm_result, {})
        assert result['assessment'] == 'Mixed stability.'

    def test_fallback(self):
        result = stability_fallback(_sample_data())
        assert 'assessment' in result
        assert len(result['concerns']) >= 1

    def test_fallback_all_stable(self):
        data = {'stability_dist': {'stable': 10, 'moderate': 0, 'volatile': 0, 'critical': 0}}
        result = stability_fallback(data)
        assert 'good' in result['recommendation'].lower()


class TestContributors:
    def test_prompt_contains_authors(self):
        prompt = contributors_prompt(_sample_data())
        assert 'Alice' in prompt
        assert 'Bob' in prompt

    def test_renderer(self):
        llm_result = {
            'overview': 'Two devs.',
            'bus_factor': 'Alice dominates.',
            'highlights': ['Alice leads'],
        }
        result = contributors_renderer(llm_result, {})
        assert result['overview'] == 'Two devs.'

    def test_fallback(self):
        result = contributors_fallback(_sample_data())
        assert result['overview']
        assert 'Alice' in result['highlights'][0]

    def test_fallback_empty(self):
        result = contributors_fallback({'authors': []})
        assert 'No contributor' in result['overview']


# ===========================================================================
# run_analysis
# ===========================================================================

class TestRunAnalysis:
    def test_fallback_only(self):
        """With no client, all sections should use fallback."""
        result = run_analysis(_sample_data(), client=None)
        # New sections
        assert 'executive' in result
        assert 'period_chapters' in result
        assert 'archaeology' in result
        assert 'ownership' in result
        assert 'project_arc' in result
        # Kept sections
        assert 'hotspots' in result
        assert 'timeline' in result
        assert 'stability' in result
        assert 'contributors' in result
        # Layer 0 data
        assert 'phases' in result
        assert 'period_themes' in result
        assert 'feature_expansion' in result
        # Verify each section has expected keys
        assert result['executive']['headline']
        assert result['hotspots']['overview']
        assert result['timeline']['narrative']
        assert result['stability']['assessment']
        assert result['contributors']['overview']
        assert 'chapters' in result['period_chapters']
        assert 'fragile_narrative' in result['archaeology']
        assert 'narrative' in result['ownership']
        assert 'narrative' in result['project_arc']

    def test_sections_count(self):
        assert len(SECTIONS) == 4  # hotspots, stability, timeline, contributors

    def test_with_repo_path(self):
        """run_analysis should accept repo_path and gather context."""
        tmp = tempfile.mkdtemp()
        try:
            result = run_analysis(_sample_data(), client=None, repo_path=tmp)
            assert 'project_context' in result
            assert result['project_context']['description'] == ''
        finally:
            shutil.rmtree(tmp)
