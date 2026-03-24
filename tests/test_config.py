"""Tests for config file diff parser."""
import json

from gitast.config import is_config_file, parse_config, flatten_dict, diff_configs


class TestIsConfigFile:
    def test_json(self):
        assert is_config_file("config.json")
        assert is_config_file("path/to/settings.json")

    def test_yaml(self):
        assert is_config_file("config.yaml")
        assert is_config_file("config.yml")

    def test_toml(self):
        assert is_config_file("pyproject.toml")

    def test_non_config(self):
        assert not is_config_file("main.py")
        assert not is_config_file("README.md")
        assert not is_config_file("Makefile")


class TestParseConfig:
    def test_json(self):
        result = parse_config('{"key": "value"}', "config.json")
        assert result == {"key": "value"}

    def test_json_nested(self):
        content = '{"a": {"b": {"c": 42}}}'
        result = parse_config(content, "config.json")
        assert result["a"]["b"]["c"] == 42

    def test_json_invalid(self):
        result = parse_config("not json {", "config.json")
        assert result is None

    def test_empty(self):
        assert parse_config("", "config.json") is None
        assert parse_config("   ", "config.json") is None

    def test_unsupported_extension(self):
        result = parse_config('{"key": "value"}', "config.txt")
        assert result is None


class TestFlattenDict:
    def test_simple(self):
        result = flatten_dict({"a": 1, "b": "hello"})
        assert result == {"a": "1", "b": "hello"}

    def test_nested(self):
        result = flatten_dict({"a": {"b": {"c": 42}}})
        assert result == {"a.b.c": "42"}

    def test_list_value(self):
        result = flatten_dict({"tags": [1, 2, 3]})
        assert result == {"tags": "[1, 2, 3]"}

    def test_null_value(self):
        result = flatten_dict({"key": None})
        assert result == {"key": "null"}

    def test_empty(self):
        assert flatten_dict({}) == {}

    def test_mixed(self):
        result = flatten_dict({
            "db": {"host": "localhost", "port": 5432},
            "debug": True,
        })
        assert result == {
            "db.host": "localhost",
            "db.port": "5432",
            "debug": "True",
        }


class TestDiffConfigs:
    def test_added_key(self):
        before = {"a": 1}
        after = {"a": 1, "b": 2}
        diffs = diff_configs(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("b", "added", None, "2")

    def test_deleted_key(self):
        before = {"a": 1, "b": 2}
        after = {"a": 1}
        diffs = diff_configs(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("b", "deleted", "2", None)

    def test_modified_key(self):
        before = {"a": 1}
        after = {"a": 2}
        diffs = diff_configs(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("a", "modified", "1", "2")

    def test_no_changes(self):
        data = {"a": 1, "b": "hello"}
        assert diff_configs(data, data) == []

    def test_nested_change(self):
        before = {"db": {"host": "localhost", "port": 5432}}
        after = {"db": {"host": "production.example.com", "port": 5432}}
        diffs = diff_configs(before, after)
        assert len(diffs) == 1
        assert diffs[0][0] == "db.host"
        assert diffs[0][1] == "modified"

    def test_non_dict_input(self):
        # YAML/JSON files can parse to non-dict (list, int, string)
        assert diff_configs(42, {"a": 1}) == [("a", "added", None, "1")]
        assert diff_configs({"a": 1}, [1, 2, 3]) == [("a", "deleted", "1", None)]
        assert diff_configs("hello", None) == []

    def test_none_before(self):
        after = {"a": 1}
        diffs = diff_configs(None, after)
        assert len(diffs) == 1
        assert diffs[0][1] == "added"

    def test_none_after(self):
        before = {"a": 1}
        diffs = diff_configs(before, None)
        assert len(diffs) == 1
        assert diffs[0][1] == "deleted"

    def test_both_none(self):
        assert diff_configs(None, None) == []

    def test_complex_diff(self):
        before = {"a": 1, "b": 2, "c": 3}
        after = {"a": 1, "b": 99, "d": 4}
        diffs = diff_configs(before, after)
        types = {d[0]: d[1] for d in diffs}
        assert types == {"b": "modified", "c": "deleted", "d": "added"}
