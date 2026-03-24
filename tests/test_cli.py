"""Tests for GitAST CLI commands"""
import os
import shutil
import tempfile

import pytest
from click.testing import CliRunner

from gitast.cli import main, _resolve_path


class TestResolvePath:
    def test_tilde_expansion(self):
        result = _resolve_path("~/myproject")
        assert "~" not in result
        assert result.startswith("/")

    def test_relative_path(self):
        result = _resolve_path(".")
        assert os.path.isabs(result)

    def test_absolute_unchanged(self):
        result = _resolve_path("/tmp/test")
        assert result == "/tmp/test"


class TestCLI:
    def setup_method(self):
        self.runner = CliRunner()
        # Use the actual gitast repo for testing
        self.repo_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def test_version(self):
        result = self.runner.invoke(main, ['--version'])
        assert result.exit_code == 0
        assert "0.6.0" in result.output

    def test_help(self):
        result = self.runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert "index" in result.output
        assert "search" in result.output
        assert "history" in result.output
        assert "blame" in result.output
        assert "status" in result.output
        assert "export" in result.output
        assert "stability" in result.output
        assert "install-hooks" in result.output
        assert "report" in result.output

    def test_index_help(self):
        result = self.runner.invoke(main, ['index', '--help'])
        assert result.exit_code == 0
        assert "--max-commits" in result.output

    def test_search_help(self):
        result = self.runner.invoke(main, ['search', '--help'])
        assert result.exit_code == 0
        assert "--limit" in result.output

    def test_index_not_a_repo(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['index', tmp])
            assert result.exit_code != 0
        finally:
            shutil.rmtree(tmp)

    def test_search_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['search', 'test', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_history_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['history', 'file.py', 'func', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_blame_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['blame', 'file.py', 'func', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_max_commits_zero_rejected(self):
        result = self.runner.invoke(main, ['index', '.', '--max-commits', '0'])
        assert result.exit_code != 0
        assert "positive" in result.output.lower()

    def test_max_commits_negative_rejected(self):
        result = self.runner.invoke(main, ['index', '.', '--max-commits', '-5'])
        assert result.exit_code != 0
        assert "positive" in result.output.lower()


class TestCLIWithIndex:
    """Tests that require an actual index. Uses a temp copy approach."""

    def setup_method(self):
        self.runner = CliRunner()
        self.tmp = tempfile.mkdtemp()
        self.repo_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_index_real_repo(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        assert "Indexing complete" in result.output
        assert "Phase 1" in result.output
        assert "Phase 5" in result.output

    def test_index_then_search(self):
        # Index first
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        # Then search
        result = self.runner.invoke(main, ['search', 'function', '-p', self.repo_path])
        assert result.exit_code == 0
        # Should find something
        assert "results" in result.output.lower() or "function" in result.output.lower()

    def test_index_then_history(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        # Query history - may or may not have data depending on what's committed
        result = self.runner.invoke(main, ['history', 'src/gitast/core.py', 'DataStore',
                                           '-p', self.repo_path])
        assert result.exit_code == 0

    def test_index_then_blame(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['blame', 'src/gitast/core.py', 'DataStore',
                                           '-p', self.repo_path])
        assert result.exit_code == 0

    def test_hotspots(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['hotspots', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_hotspots_with_filters(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['hotspots', '--file', 'core.py',
                                           '-k', '5', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_blame_summary(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['blame-summary', 'src/gitast/core.py',
                                           '-p', self.repo_path])
        assert result.exit_code == 0
        assert "core.py" in result.output
        assert "DataStore" in result.output

    def test_hotspots_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['hotspots', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_blame_summary_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['blame-summary', 'file.py', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_find_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        result = self.runner.invoke(main, ['find', 'DataStore', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "DataStore" in result.output

    def test_find_with_kind_filter(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        result = self.runner.invoke(main, ['find', 'DataStore', '-t', 'class', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_age_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        result = self.runner.invoke(main, ['age', '-k', '5', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_timeline_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        result = self.runner.invoke(main, ['timeline', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_diff_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        # Use a non-existent hash — should still exit 0 with "No function changes"
        result = self.runner.invoke(main, ['diff', 'zzz', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "No function changes" in result.output

    def test_file_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        result = self.runner.invoke(main, ['file', 'core.py', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "core.py" in result.output

    def test_find_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['find', 'foo', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_age_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['age', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_timeline_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['timeline', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_diff_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['diff', 'abc', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_file_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['file', 'foo.py', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    # -- v0.2 incremental index + status tests --

    def test_index_force(self):
        """--force should do a full reindex."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        assert "Full reindex requested" in result.output
        assert "Indexing complete" in result.output

    def test_status_with_index(self):
        """Status should show index freshness after indexing."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['status', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "Index Status" in result.output
        assert "up to date" in result.output

    def test_status_no_index(self):
        """Status without index should fail gracefully."""
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['status', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_index_incremental_up_to_date(self):
        """Re-running index without changes should say up to date."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        assert "Indexing complete" in result.output

        # Second run — should detect up to date
        result = self.runner.invoke(main, ['index', self.repo_path])
        assert result.exit_code == 0
        assert "up to date" in result.output

    def test_index_then_force_reindex(self):
        """Force after incremental should work."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0
        assert "Full reindex requested" in result.output
        assert "Indexing complete" in result.output

    def test_index_help_shows_force(self):
        result = self.runner.invoke(main, ['index', '--help'])
        assert result.exit_code == 0
        assert "--force" in result.output

    def test_export_json(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        import tempfile
        outfile = os.path.join(self.tmp, 'export.json')
        result = self.runner.invoke(main, ['export', 'json', '-o', outfile,
                                           '-i', 'functions', '-p', self.repo_path])
        assert result.exit_code == 0
        assert os.path.exists(outfile)
        import json
        with open(outfile) as f:
            data = json.load(f)
        assert 'functions' in data
        assert len(data['functions']) > 0

    def test_export_csv(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        outfile = os.path.join(self.tmp, 'export.csv')
        result = self.runner.invoke(main, ['export', 'csv', '-o', outfile,
                                           '-i', 'functions', '-p', self.repo_path])
        assert result.exit_code == 0
        assert os.path.exists(outfile)

    def test_export_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['export', 'json', '-o', '/tmp/x.json', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_stability_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['stability', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_stability_volatile(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['stability', '--volatile', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_stability_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['stability', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_install_hooks(self):
        result = self.runner.invoke(main, ['install-hooks', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "Installed hooks" in result.output

        # Second install should say already installed
        result = self.runner.invoke(main, ['install-hooks', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "already installed" in result.output

    def test_uninstall_hooks(self):
        # Install first
        self.runner.invoke(main, ['install-hooks', '-p', self.repo_path])

        result = self.runner.invoke(main, ['uninstall-hooks', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "Removed hooks" in result.output

    def test_uninstall_hooks_none(self):
        result = self.runner.invoke(main, ['uninstall-hooks', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "No GitAST hooks" in result.output

    def test_report_generates_html(self):
        # Index first
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        output_file = os.path.join(self.tmp, 'test-report.html')
        result = self.runner.invoke(main, ['report', '-p', self.repo_path, '-o', output_file])
        assert result.exit_code == 0
        assert "Report generated" in result.output
        assert os.path.exists(output_file)

        with open(output_file, 'r') as f:
            html = f.read()
        assert '<!DOCTYPE html>' in html
        assert 'chart.js' in html
        assert 'GitAST Report' in html
        assert 'timelineChart' in html

    def test_report_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['report', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    # -- v0.5.0 new command tests --

    def test_cat_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['cat', 'HEAD', 'src/gitast/models.py', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_cat_bad_commit(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['cat', 'nonexistent999', 'foo.py', '-p', self.repo_path])
        assert result.exit_code != 0

    def test_cat_bad_file(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['cat', 'HEAD', 'does/not/exist.py', '-p', self.repo_path])
        assert result.exit_code != 0

    def test_langs_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['langs', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "Languages" in result.output
        assert "python" in result.output.lower()

    def test_langs_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['langs', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_fragile_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['fragile', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_fragile_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['fragile', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_stale_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['stale', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_stale_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['stale', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_commits_grep(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['commits', '--grep', 'fix', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_commits_grep_no_results(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['commits', '--grep', 'zzzznonexistent', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_search_type_filter(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['search', 'function', '--type', 'function', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_search_type_commit(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['search', 'fix', '--type', 'commit', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_diff_filter(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['diff', 'zzz', '--filter', 'core', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_history_function_name_only(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['history', 'DataStore', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_history_backward_compat(self):
        """Old-style: gitast history file_path function_name still works."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['history', 'src/gitast/core.py', 'DataStore', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_find_deleted(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['find', 'anything', '--deleted', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_report_default_output(self):
        # Index first
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['report', '-p', self.repo_path])
        assert result.exit_code == 0
        assert "Report generated" in result.output

    def test_report_no_llm_flag(self):
        """--no-llm should skip LLM analysis and still generate report."""
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        output_file = os.path.join(self.tmp, 'nollm-report.html')
        result = self.runner.invoke(main, ['report', '-p', self.repo_path, '-o', output_file, '--no-llm'])
        assert result.exit_code == 0
        assert "Report generated" in result.output
        assert os.path.exists(output_file)

    def test_report_help_shows_llm_options(self):
        result = self.runner.invoke(main, ['report', '--help'])
        assert result.exit_code == 0
        assert '--no-llm' in result.output
        assert '--remote' in result.output
        assert '--llm-endpoint' in result.output
        assert '--llm-model' in result.output

    def test_search_json_output(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['search', 'function', '--json-output', '-p', self.repo_path])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, list)

    def test_hotspots_json_output(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['hotspots', '--json-output', '-p', self.repo_path])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, list)

    def test_status_json_output(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['status', '--json-output', '-p', self.repo_path])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, dict)
        assert 'stats' in data

    def test_summary_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['summary', '-p', self.repo_path])
        assert result.exit_code == 0
        assert 'Codebase Summary' in result.output

    def test_risks_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '2', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['risks', '-p', self.repo_path])
        assert result.exit_code == 0

    # -- v0.5.2 new command tests --

    def test_coupled_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['coupled', 'DataStore', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_coupled_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['coupled', 'foo', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_changed_since_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['changed-since', '365d', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_changed_since_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['changed-since', '30d', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_file_history_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['file-history', 'core.py', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_file_history_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['file-history', 'foo.py', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_churn_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['churn', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_churn_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['churn', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_why_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['why', 'DataStore', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_why_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['why', 'foo', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_untested_command(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['untested', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_untested_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['untested', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_commits_regex_grep(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['commits', '--grep', '(fix|add)', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_releases_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['releases', '-p', self.repo_path])
        assert result.exit_code == 0
        # Repo has v0.4.0 and v0.5.0 tags
        assert "v0.4.0" in result.output or "v0.5.0" in result.output or "Releases" in result.output or "No tags" in result.output

    def test_releases_json(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['releases', '-p', self.repo_path, '--json-output'])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, list)

    def test_releases_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['releases', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_diff_with_tag_names(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '10', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['diff', 'v0.4.0', 'v0.5.0', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_track_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        # Search for any key — may or may not find results depending on config files
        result = self.runner.invoke(main, ['track', 'version', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_track_json(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['track', 'nonexistent_key_xyz', '-p', self.repo_path, '--json-output'])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, list)

    def test_track_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['track', 'key', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_config_keys_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['config-keys', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_config_keys_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['config-keys', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_deps_with_index(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['deps', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_deps_summary(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['deps', '--summary', '-p', self.repo_path])
        assert result.exit_code == 0

    def test_deps_json(self):
        result = self.runner.invoke(main, ['index', self.repo_path, '--max-commits', '5', '--force'])
        assert result.exit_code == 0

        result = self.runner.invoke(main, ['deps', '-p', self.repo_path, '--json-output'])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert isinstance(data, list)

    def test_deps_no_index(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['deps', '-p', tmp])
            assert result.exit_code != 0
            assert "No index found" in result.output
        finally:
            shutil.rmtree(tmp)

    def test_install_hooks_not_a_repo(self):
        tmp = tempfile.mkdtemp()
        try:
            result = self.runner.invoke(main, ['install-hooks', '-p', tmp])
            assert result.exit_code != 0
        finally:
            shutil.rmtree(tmp)
