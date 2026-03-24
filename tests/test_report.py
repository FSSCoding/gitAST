"""Tests for GitAST HTML report generation"""
import os
import tempfile
import shutil

from gitast.report import generate_report, _prepare_data, _escape_html


class TestReportGeneration:
    def setup_method(self):
        self.tmp = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _sample_data(self):
        return {
            'stats': {'commits': 50, 'functions': 100, 'changes': 200, 'blame_entries': 150},
            'timeline': [
                {'month': '2024-01', 'commits': 10, 'changes': 20, 'functions': 15, 'authors': 3},
                {'month': '2024-02', 'commits': 8, 'changes': 15, 'functions': 10, 'authors': 2},
            ],
            'hotspots': [
                {'function_name': 'parse', 'file_path': 'src/parser.py', 'change_count': 15, 'author_count': 3},
            ],
            'stability': [
                {'function_name': 'stable_fn', 'file_path': 'a.py', 'stability_score': 0.95, 'rating': 'stable', 'change_count': 1},
                {'function_name': 'volatile_fn', 'file_path': 'b.py', 'stability_score': 0.2, 'rating': 'critical', 'change_count': 20},
            ],
            'authors': [
                {'author': 'Alice', 'change_count': 50, 'functions_touched': 30, 'files_touched': 10, 'first_commit': '2024-01-01'},
            ],
            'languages': [
                {'language': 'python', 'count': 80},
                {'language': 'javascript', 'count': 20},
            ],
        }

    def test_generate_report_creates_file(self):
        output = os.path.join(self.tmp, 'report.html')
        generate_report(self._sample_data(), 'test-repo', output)
        assert os.path.exists(output)

        with open(output, 'r') as f:
            html = f.read()
        assert '<!DOCTYPE html>' in html
        assert 'test-repo' in html
        assert 'chart.js' in html

    def test_generate_report_contains_data(self):
        output = os.path.join(self.tmp, 'report.html')
        generate_report(self._sample_data(), 'my-project', output)

        with open(output, 'r') as f:
            html = f.read()
        assert 'my-project' in html
        assert '"commits": 50' in html
        assert '"python"' in html
        assert '"Alice"' in html

    def test_generate_report_subdirectory(self):
        output = os.path.join(self.tmp, 'sub', 'dir', 'report.html')
        generate_report(self._sample_data(), 'test', output)
        assert os.path.exists(output)

    def test_generate_report_empty_data(self):
        output = os.path.join(self.tmp, 'empty.html')
        data = {
            'stats': {'commits': 0, 'functions': 0, 'changes': 0, 'blame_entries': 0},
            'timeline': [],
            'hotspots': [],
            'stability': [],
            'authors': [],
            'languages': [],
        }
        generate_report(data, 'empty-repo', output)
        assert os.path.exists(output)
        with open(output, 'r') as f:
            html = f.read()
        assert '<!DOCTYPE html>' in html


class TestPrepareData:
    def test_stability_distribution(self):
        data = {
            'stats': {},
            'timeline': [],
            'hotspots': [],
            'stability': [
                {'rating': 'stable', 'change_count': 1},
                {'rating': 'stable', 'change_count': 2},
                {'rating': 'moderate', 'change_count': 3},
                {'rating': 'volatile', 'change_count': 5},
                {'rating': 'critical', 'change_count': 8},
                {'rating': 'critical', 'change_count': 10},
            ],
            'authors': [],
            'languages': [],
        }
        result = _prepare_data(data)
        assert result['stability_dist'] == {'stable': 2, 'moderate': 1, 'volatile': 1, 'critical': 2}

    def test_empty_stability(self):
        data = {
            'stats': {},
            'timeline': [],
            'hotspots': [],
            'stability': [],
            'authors': [],
            'languages': [],
        }
        result = _prepare_data(data)
        assert result['stability_dist'] == {'stable': 0, 'moderate': 0, 'volatile': 0, 'critical': 0}


class TestEscapeHtml:
    def test_special_chars(self):
        assert _escape_html('<script>alert("xss")</script>') == '&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;'

    def test_ampersand(self):
        assert _escape_html('A & B') == 'A &amp; B'

    def test_plain_text(self):
        assert _escape_html('hello world') == 'hello world'
