"""Tests for GitAST analysis: ASTParser and BlameAnalyzer"""
from unittest.mock import patch
from gitast.analysis import ASTParser, BlameAnalyzer
from gitast.models import FunctionInfo


SAMPLE_PYTHON = '''
import os

def hello(name: str) -> str:
    """Greet someone."""
    return f"Hello, {name}!"

class UserService:
    def __init__(self, db):
        self.db = db

    def get_user(self, user_id: int):
        return self.db.query(user_id)

    def delete_user(self, user_id: int):
        self.db.delete(user_id)

def standalone():
    pass
'''

SAMPLE_JS = '''
function processData(input) {
    const result = input.map(x => x * 2);
    return result;
}

class DataService {
    constructor(config) {
        this.config = config;
    }

    fetch(url) {
        return this.config.client.get(url);
    }
}

function helper() {
    return true;
}
'''


class TestASTParser:
    def setup_method(self):
        self.parser = ASTParser()

    def test_parse_python_functions(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        names = [f.name for f in funcs]
        assert "hello" in names
        assert "standalone" in names

    def test_parse_python_classes(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        classes = [f for f in funcs if f.kind == "class"]
        class_names = [c.name for c in classes]
        assert "UserService" in class_names

    def test_parse_python_line_ranges(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        hello = next(f for f in funcs if f.name == "hello")
        assert hello.start_line > 0
        assert hello.end_line > hello.start_line
        assert hello.language == "python"

    def test_parse_python_all_functions(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        func_names = [f.name for f in funcs if f.kind in ("function", "method")]
        assert len(func_names) >= 3

    def test_parse_javascript(self):
        funcs = self.parser.parse_file(SAMPLE_JS, "app.js", "javascript")
        names = [f.name for f in funcs]
        assert "processData" in names
        assert "helper" in names
        assert "DataService" in names

    def test_parse_empty_file(self):
        funcs = self.parser.parse_file("", "empty.py", "python")
        assert funcs == []

    def test_parse_whitespace_only(self):
        funcs = self.parser.parse_file("   \n\n  \n", "blank.py", "python")
        assert funcs == []

    def test_file_path_preserved(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "src/app/main.py", "python")
        for f in funcs:
            assert f.file_path == "src/app/main.py"

    def test_signature_populated(self):
        funcs = self.parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        hello = next(f for f in funcs if f.name == "hello")
        assert "def hello" in hello.signature


class TestFallbackParser:
    """Test the regex-based fallback parser by mocking out tree-sitter imports."""

    def setup_method(self):
        self.parser = ASTParser()

    def _parse_with_fallback(self, source, file_path, language):
        """Force fallback by calling _fallback_parse directly."""
        return self.parser._fallback_parse(source, file_path, language)

    def test_fallback_python_functions(self):
        funcs = self._parse_with_fallback(SAMPLE_PYTHON, "test.py", "python")
        names = [f.name for f in funcs]
        assert "hello" in names
        assert "standalone" in names

    def test_fallback_python_classes(self):
        funcs = self._parse_with_fallback(SAMPLE_PYTHON, "test.py", "python")
        classes = [f for f in funcs if f.kind == "class"]
        assert any(c.name == "UserService" for c in classes)

    def test_fallback_python_line_ranges(self):
        funcs = self._parse_with_fallback(SAMPLE_PYTHON, "test.py", "python")
        hello = next(f for f in funcs if f.name == "hello")
        assert hello.start_line > 0
        assert hello.end_line >= hello.start_line

    def test_fallback_javascript(self):
        funcs = self._parse_with_fallback(SAMPLE_JS, "app.js", "javascript")
        names = [f.name for f in funcs]
        assert "processData" in names
        assert "helper" in names

    def test_fallback_js_classes(self):
        funcs = self._parse_with_fallback(SAMPLE_JS, "app.js", "javascript")
        classes = [f for f in funcs if f.kind == "class"]
        assert any(c.name == "DataService" for c in classes)

    def test_fallback_typescript(self):
        ts_code = '''
export function fetchData(url: string): Promise<Response> {
    return fetch(url);
}

export class ApiClient {
    constructor(private baseUrl: string) {}
}
'''
        funcs = self._parse_with_fallback(ts_code, "api.ts", "typescript")
        names = [f.name for f in funcs]
        assert "fetchData" in names
        assert "ApiClient" in names

    def test_fallback_empty(self):
        funcs = self._parse_with_fallback("", "empty.py", "python")
        assert funcs == []

    def test_fallback_unsupported_language(self):
        funcs = self._parse_with_fallback("void main() {}", "main.zig", "zig")
        assert funcs == []

    def test_fallback_unbalanced_braces(self):
        """Unbalanced braces should not cause infinite scan; capped at 1000 lines."""
        # Function with opening brace but no matching close
        lines = ['function broken() {', '  console.log("hi");']
        lines.extend(['  // filler'] * 50)
        code = '\n'.join(lines)
        funcs = self._parse_with_fallback(code, "bad.js", "javascript")
        assert len(funcs) == 1
        # end_line should not exceed the actual file length
        assert funcs[0].end_line <= len(lines)

    def test_fallback_braces_in_strings_ignored(self):
        """Braces inside string literals and comments should not affect depth counting."""
        code = '''function render() {
    console.log("}}}}");
    let x = '{{{';
    // }}}
    return true;
}

function other() {
    return 1;
}
'''
        funcs = self._parse_with_fallback(code, "test.js", "javascript")
        names = [f.name for f in funcs]
        assert "render" in names
        assert "other" in names
        render = next(f for f in funcs if f.name == "render")
        # Verify render ends before other starts (braces in strings didn't break counting)
        other = next(f for f in funcs if f.name == "other")
        assert render.end_line < other.start_line

    def test_fallback_arrow_functions(self):
        """Fallback parser should detect const arrow functions."""
        code = '''
const fetchData = async (url) => {
    const response = await fetch(url);
    return response.json();
}

const add = (a, b) => {
    return a + b;
}

export const multiply = x => {
    return x * 2;
}
'''
        funcs = self._parse_with_fallback(code, "utils.js", "javascript")
        names = [f.name for f in funcs]
        assert "fetchData" in names
        assert "add" in names
        assert "multiply" in names

    def test_invalid_names_rejected_by_treesitter_parser(self):
        """Names with spaces, newlines, or non-identifier chars must be filtered out."""
        parser = ASTParser()
        # Even if tree-sitter somehow returns a weird name node, it should be rejected
        funcs = parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
        for f in funcs:
            assert '\n' not in f.name, f"Name contains newline: {f.name!r}"
            assert ' ' not in f.name, f"Name contains space: {f.name!r}"
            # Must be a valid identifier
            import re
            assert re.match(r'^[A-Za-z_]\w*$', f.name), f"Not a valid identifier: {f.name!r}"

    def test_fallback_used_when_treesitter_unavailable(self):
        """Verify that parse_file falls back when tree-sitter import fails."""
        parser = ASTParser()
        with patch.object(parser, '_get_parser', side_effect=ImportError("no module")):
            funcs = parser.parse_file(SAMPLE_PYTHON, "test.py", "python")
            names = [f.name for f in funcs]
            assert "hello" in names


class TestNewLanguages:
    """Tests for Rust, Go, Java, C, C++ parsing."""

    def setup_method(self):
        self.parser = ASTParser()

    def test_rust_functions_and_structs(self):
        src = '''pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

struct Point {
    x: f64,
    y: f64,
}
'''
        funcs = self.parser.parse_file(src, 'lib.rs', 'rust')
        names = {f.name: f for f in funcs}
        assert 'add' in names
        assert names['add'].kind == 'function'
        assert 'Point' in names
        assert names['Point'].kind == 'class'

    def test_rust_impl_methods(self):
        src = '''impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Point { x, y }
    }

    fn distance(&self) -> f64 {
        (self.x.powi(2) + self.y.powi(2)).sqrt()
    }
}
'''
        funcs = self.parser.parse_file(src, 'lib.rs', 'rust')
        methods = [f for f in funcs if f.kind == 'method']
        assert len(methods) == 2
        method_names = {m.name for m in methods}
        assert 'new' in method_names
        assert 'distance' in method_names

    def test_rust_enum_and_trait(self):
        src = '''enum Color {
    Red,
    Green,
}

trait Drawable {
    fn draw(&self);
}
'''
        funcs = self.parser.parse_file(src, 'lib.rs', 'rust')
        names = {f.name: f for f in funcs}
        assert 'Color' in names
        assert names['Color'].kind == 'class'
        assert 'Drawable' in names

    def test_go_functions_and_types(self):
        src = '''func Add(a, b int) int {
    return a + b
}

type Server struct {
    port int
}
'''
        funcs = self.parser.parse_file(src, 'main.go', 'go')
        names = {f.name: f for f in funcs}
        assert 'Add' in names
        assert names['Add'].kind == 'function'
        assert 'Server' in names
        assert names['Server'].kind == 'class'

    def test_go_methods_with_receiver(self):
        src = '''func (s *Server) Start() error {
    return nil
}

func (s Server) GetPort() int {
    return s.port
}
'''
        funcs = self.parser.parse_file(src, 'server.go', 'go')
        assert len(funcs) == 2
        assert all(f.kind == 'method' for f in funcs)
        names = {f.name for f in funcs}
        assert 'Start' in names
        assert 'GetPort' in names

    def test_java_class_and_methods(self):
        src = '''public class Calculator {
    public int add(int a, int b) {
        return a + b;
    }

    private void reset() {
        // reset
    }
}
'''
        funcs = self.parser.parse_file(src, 'Calculator.java', 'java')
        names = {f.name: f for f in funcs}
        assert 'Calculator' in names
        assert names['Calculator'].kind == 'class'
        assert 'add' in names
        assert names['add'].kind == 'method'
        assert 'reset' in names

    def test_java_interface(self):
        src = '''public interface Searchable {
    void search(String query);
}
'''
        funcs = self.parser.parse_file(src, 'Searchable.java', 'java')
        names = {f.name for f in funcs}
        assert 'Searchable' in names

    def test_c_functions_and_structs(self):
        src = '''struct Point {
    int x;
    int y;
};

int add(int a, int b) {
    return a + b;
}
'''
        funcs = self.parser.parse_file(src, 'math.c', 'c')
        names = {f.name: f for f in funcs}
        assert 'Point' in names
        assert names['Point'].kind == 'class'
        assert 'add' in names
        assert names['add'].kind == 'function'

    def test_cpp_class_and_methods(self):
        src = '''class Calculator {
public:
    int add(int a, int b) {
        return a + b;
    }
};
'''
        funcs = self.parser.parse_file(src, 'calc.cpp', 'cpp')
        names = {f.name: f for f in funcs}
        assert 'Calculator' in names
        assert names['Calculator'].kind == 'class'
        assert 'add' in names
        assert names['add'].kind == 'method'

    def test_rust_fallback(self):
        src = '''pub fn hello() {
    println!("hi");
}

struct Foo {
    bar: i32,
}
'''
        with patch.object(self.parser, '_get_parser', side_effect=ImportError):
            funcs = self.parser.parse_file(src, 'lib.rs', 'rust')
        names = {f.name for f in funcs}
        assert 'hello' in names
        assert 'Foo' in names

    def test_go_fallback(self):
        src = '''func Hello() {
    fmt.Println("hi")
}

type MyStruct struct {
    x int
}
'''
        with patch.object(self.parser, '_get_parser', side_effect=ImportError):
            funcs = self.parser.parse_file(src, 'main.go', 'go')
        names = {f.name for f in funcs}
        assert 'Hello' in names
        assert 'MyStruct' in names

    def test_c_fallback(self):
        src = '''int main(int argc, char *argv[]) {
    return 0;
}

struct Data {
    int value;
};
'''
        with patch.object(self.parser, '_get_parser', side_effect=ImportError):
            funcs = self.parser.parse_file(src, 'main.c', 'c')
        names = {f.name for f in funcs}
        assert 'main' in names
        assert 'Data' in names


class TestBlameAnalyzer:
    def setup_method(self):
        self.analyzer = BlameAnalyzer()

    def test_basic_blame(self):
        func = FunctionInfo(
            name="hello", file_path="test.py", language="python",
            start_line=3, end_line=5, kind="function",
        )
        blame_data = [
            ("aaa", "Alice", 1),
            ("aaa", "Alice", 2),
            ("aaa", "Alice", 3),  # in function
            ("bbb", "Bob", 4),    # in function
            ("bbb", "Bob", 5),    # in function
            ("aaa", "Alice", 6),
        ]
        entries = self.analyzer.analyze_function_blame(blame_data, func)
        assert len(entries) == 2

        bob = next(e for e in entries if e.author == "Bob")
        alice = next(e for e in entries if e.author == "Alice")
        assert bob.line_count == 2
        assert alice.line_count == 1
        assert bob.percentage > alice.percentage

    def test_single_author(self):
        func = FunctionInfo(
            name="f", file_path="x.py", language="python",
            start_line=1, end_line=3, kind="function",
        )
        blame_data = [
            ("aaa", "Alice", 1),
            ("aaa", "Alice", 2),
            ("aaa", "Alice", 3),
        ]
        entries = self.analyzer.analyze_function_blame(blame_data, func)
        assert len(entries) == 1
        assert entries[0].percentage == 100.0

    def test_no_blame_in_range(self):
        func = FunctionInfo(
            name="f", file_path="x.py", language="python",
            start_line=100, end_line=110, kind="function",
        )
        blame_data = [("aaa", "Alice", 1), ("aaa", "Alice", 2)]
        entries = self.analyzer.analyze_function_blame(blame_data, func)
        assert entries == []

    def test_most_common_commit_tracked(self):
        """Blame entry should track the most common commit for an author."""
        func = FunctionInfo(
            name="f", file_path="x.py", language="python",
            start_line=1, end_line=5, kind="function",
        )
        blame_data = [
            ("commit_a", "Alice", 1),
            ("commit_a", "Alice", 2),
            ("commit_a", "Alice", 3),
            ("commit_b", "Alice", 4),
            ("commit_b", "Alice", 5),
        ]
        entries = self.analyzer.analyze_function_blame(blame_data, func)
        assert len(entries) == 1
        # commit_a has 3 lines, commit_b has 2 - should pick commit_a
        assert entries[0].commit_hash == "commit_a"

    def test_sorted_by_percentage(self):
        func = FunctionInfo(
            name="f", file_path="x.py", language="python",
            start_line=1, end_line=4, kind="function",
        )
        blame_data = [
            ("a", "Alice", 1),
            ("b", "Bob", 2),
            ("b", "Bob", 3),
            ("b", "Bob", 4),
        ]
        entries = self.analyzer.analyze_function_blame(blame_data, func)
        assert entries[0].author == "Bob"
        assert entries[1].author == "Alice"
