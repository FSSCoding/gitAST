"""Tests for dependency file parser."""
from gitast.deps import is_dep_file, parse_deps, diff_deps


class TestIsDepFile:
    def test_requirements(self):
        assert is_dep_file("requirements.txt")
        assert is_dep_file("requirements-dev.txt")
        assert is_dep_file("path/to/requirements.txt")

    def test_package_json(self):
        assert is_dep_file("package.json")
        assert is_dep_file("frontend/package.json")

    def test_pyproject(self):
        assert is_dep_file("pyproject.toml")

    def test_cargo(self):
        assert is_dep_file("Cargo.toml")

    def test_gomod(self):
        assert is_dep_file("go.mod")

    def test_non_dep(self):
        assert not is_dep_file("main.py")
        assert not is_dep_file("config.json")
        assert not is_dep_file("Makefile")


class TestParseRequirementsTxt:
    def test_basic(self):
        content = "requests==2.28.0\nflask>=2.0\nnumpy\n"
        deps = parse_deps(content, "requirements.txt")
        assert deps["requests"] == "==2.28.0"
        assert deps["flask"] == ">=2.0"
        assert deps["numpy"] == "*"

    def test_comments_and_blanks(self):
        content = "# comment\nrequests==1.0\n\n# another\nflask>=2.0\n"
        deps = parse_deps(content, "requirements.txt")
        assert len(deps) == 2

    def test_dash_options(self):
        content = "-r base.txt\n--index-url https://pypi.org\nrequests==1.0\n"
        deps = parse_deps(content, "requirements.txt")
        assert len(deps) == 1
        assert "requests" in deps

    def test_normalisation(self):
        content = "Flask_Login==0.6\n"
        deps = parse_deps(content, "requirements.txt")
        assert "flask-login" in deps

    def test_extras_bracket(self):
        content = "requests[security]>=2.0\n"
        deps = parse_deps(content, "requirements.txt")
        assert deps["requests"] == ">=2.0"

    def test_git_url_skipped(self):
        content = "git+https://github.com/user/repo.git\nrequests==1.0\n"
        deps = parse_deps(content, "requirements.txt")
        assert len(deps) == 1
        assert "requests" in deps
        assert "git" not in deps

    def test_inline_comment(self):
        content = "requests==2.0  # pinned for compat\n"
        deps = parse_deps(content, "requirements.txt")
        assert deps["requests"] == "==2.0"

    def test_compound_version(self):
        content = "requests>=2.0,<3.0\n"
        deps = parse_deps(content, "requirements.txt")
        assert deps["requests"] == ">=2.0,<3.0"

    def test_garbage_line(self):
        content = "also broken\n"
        deps = parse_deps(content, "requirements.txt")
        assert len(deps) == 0


class TestParsePackageJson:
    def test_basic(self):
        content = '{"dependencies": {"react": "^18.0", "lodash": "4.17.21"}, "devDependencies": {"jest": "^29.0"}}'
        deps = parse_deps(content, "package.json")
        assert deps["react"] == "^18.0"
        assert deps["lodash"] == "4.17.21"
        assert deps["jest"] == "^29.0"

    def test_empty(self):
        deps = parse_deps('{}', "package.json")
        assert deps == {}

    def test_invalid_json(self):
        deps = parse_deps("not json", "package.json")
        assert deps == {}

    def test_peer_deps(self):
        content = '{"peerDependencies": {"react": ">=16"}}'
        deps = parse_deps(content, "package.json")
        assert deps["react"] == ">=16"


class TestParsePyprojectToml:
    def test_pep621(self):
        content = '[project]\ndependencies = ["requests>=2.0", "click~=8.0"]\n'
        deps = parse_deps(content, "pyproject.toml")
        assert "requests" in deps
        assert "click" in deps

    def test_optional_deps(self):
        content = '[project.optional-dependencies]\ndev = ["pytest>=7.0"]\n'
        deps = parse_deps(content, "pyproject.toml")
        assert "pytest" in deps


class TestParseGoMod:
    def test_block_require(self):
        content = """module example.com/myapp

go 1.21

require (
\tgithub.com/gin-gonic/gin v1.9.1
\tgithub.com/lib/pq v1.10.9
)
"""
        deps = parse_deps(content, "go.mod")
        assert deps["github.com/gin-gonic/gin"] == "v1.9.1"
        assert deps["github.com/lib/pq"] == "v1.10.9"

    def test_single_require(self):
        content = "module example.com/app\n\nrequire github.com/pkg/errors v0.9.1\n"
        deps = parse_deps(content, "go.mod")
        assert deps["github.com/pkg/errors"] == "v0.9.1"

    def test_indirect_comment_stripped(self):
        content = """module example.com/app

require (
\tgithub.com/direct v1.0.0
\tgithub.com/indirect v2.0.0 // indirect
)
"""
        deps = parse_deps(content, "go.mod")
        assert deps["github.com/indirect"] == "v2.0.0"


class TestDiffDeps:
    def test_added(self):
        before = {"requests": "==1.0"}
        after = {"requests": "==1.0", "flask": ">=2.0"}
        diffs = diff_deps(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("flask", "added", None, ">=2.0")

    def test_removed(self):
        before = {"requests": "==1.0", "flask": ">=2.0"}
        after = {"requests": "==1.0"}
        diffs = diff_deps(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("flask", "removed", ">=2.0", None)

    def test_bumped(self):
        before = {"requests": "==1.0"}
        after = {"requests": "==2.0"}
        diffs = diff_deps(before, after)
        assert len(diffs) == 1
        assert diffs[0] == ("requests", "bumped", "==1.0", "==2.0")

    def test_no_changes(self):
        deps = {"requests": "==1.0"}
        assert diff_deps(deps, deps) == []

    def test_complex(self):
        before = {"a": "1.0", "b": "2.0", "c": "3.0"}
        after = {"a": "1.0", "b": "2.1", "d": "4.0"}
        diffs = diff_deps(before, after)
        types = {d[0]: d[1] for d in diffs}
        assert types == {"b": "bumped", "c": "removed", "d": "added"}
