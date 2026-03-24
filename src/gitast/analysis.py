"""AST parsing and blame analysis for GitAST"""
import re
from collections import defaultdict
from typing import List, Optional, Dict

from .models import FunctionInfo, BlameEntry

_VALID_IDENTIFIER = re.compile(r'^[A-Za-z_]\w*$')


# Node types that represent function/class definitions per language
LANGUAGE_NODE_TYPES = {
    'python': {
        'function': ['function_definition'],
        'class': ['class_definition'],
        'name_field': 'name',
    },
    'javascript': {
        'function': ['function_declaration', 'method_definition'],
        'class': ['class_declaration'],
        'name_field': 'name',
    },
    'typescript': {
        'function': ['function_declaration', 'method_definition'],
        'class': ['class_declaration'],
        'name_field': 'name',
    },
    'rust': {
        'function': ['function_item'],
        'class': ['struct_item', 'enum_item', 'trait_item', 'impl_item'],
        'name_field': 'name',
    },
    'go': {
        'function': ['function_declaration', 'method_declaration'],
        'class': ['type_declaration'],
        'name_field': 'name',
    },
    'java': {
        'function': ['method_declaration', 'constructor_declaration'],
        'class': ['class_declaration', 'interface_declaration', 'enum_declaration'],
        'name_field': 'name',
    },
    'c': {
        'function': ['function_definition'],
        'class': ['struct_specifier', 'enum_specifier'],
        'name_field': 'declarator',
    },
    'cpp': {
        'function': ['function_definition'],
        'class': ['struct_specifier', 'class_specifier', 'enum_specifier'],
        'name_field': 'declarator',
    },
}


class ASTParser:
    """Extracts function/class definitions from source code using tree-sitter."""

    def __init__(self):
        self._parsers = {}

    def _get_parser(self, language: str):
        """Lazily initialize tree-sitter parser for a language."""
        if language in self._parsers:
            return self._parsers[language]

        import tree_sitter
        import importlib

        module_map = {
            'python': 'tree_sitter_python',
            'javascript': 'tree_sitter_javascript',
            'typescript': 'tree_sitter_typescript',
            'rust': 'tree_sitter_rust',
            'go': 'tree_sitter_go',
            'java': 'tree_sitter_java',
            'c': 'tree_sitter_c',
            'cpp': 'tree_sitter_cpp',
        }
        mod_name = module_map.get(language)
        if not mod_name:
            raise ValueError(f"Unsupported language: {language}")

        lang_mod = importlib.import_module(mod_name)

        # tree-sitter-typescript has separate language() for ts vs tsx
        if language == 'typescript' and hasattr(lang_mod, 'language_typescript'):
            ts_lang = tree_sitter.Language(lang_mod.language_typescript())
        else:
            ts_lang = tree_sitter.Language(lang_mod.language())

        parser = tree_sitter.Parser(ts_lang)
        self._parsers[language] = parser
        return parser

    def parse_file(self, source: str, file_path: str, language: str) -> List[FunctionInfo]:
        """Parse source code and extract function/class definitions."""
        if not source.strip():
            return []

        try:
            parser = self._get_parser(language)
        except (ImportError, ValueError):
            return self._fallback_parse(source, file_path, language)

        tree = parser.parse(source.encode('utf-8'))
        node_types = LANGUAGE_NODE_TYPES.get(language, {})
        func_types = set(node_types.get('function', []))
        class_types = set(node_types.get('class', []))
        name_field = node_types.get('name_field', 'name')
        target_types = func_types | class_types

        results = []
        self._walk_tree(tree.root_node, source, file_path, language,
                        func_types, class_types, name_field, results)
        return results

    def _extract_name(self, node, name_field: str, source: str) -> Optional[str]:
        """Extract identifier name from a node, handling language-specific AST layouts."""
        name_node = node.child_by_field_name(name_field)
        if not name_node:
            # Fallback: look for type_identifier or type_spec children (Go, C/C++ structs)
            for child in node.children:
                if child.type == 'type_identifier':
                    name_node = child
                    break
                elif child.type == 'type_spec':
                    # Go: type_declaration → type_spec → type_identifier
                    for gc in child.children:
                        if gc.type == 'type_identifier':
                            name_node = gc
                            break
                    break
            if not name_node:
                return None
        # C/C++: declarator can be nested (pointer_declarator, function_declarator, etc.)
        while name_node.type.endswith('_declarator') or name_node.type == 'qualified_identifier':
            child = (name_node.child_by_field_name('declarator')
                     or name_node.child_by_field_name('name'))
            if child:
                name_node = child
            else:
                break
        # C++ methods inside classes use field_identifier instead of identifier
        if name_node.type == 'field_identifier':
            pass  # valid — use it
        name = source[name_node.start_byte:name_node.end_byte]
        return name if _VALID_IDENTIFIER.match(name) else None

    def _walk_tree(self, node, source: str, file_path: str, language: str,
                   func_types: set, class_types: set, name_field: str,
                   results: List[FunctionInfo]) -> None:
        """Recursively walk the AST and extract definitions."""
        if node.type in func_types or node.type in class_types:
            name = self._extract_name(node, name_field, source)
            if name:
                start_line = node.start_point[0] + 1
                end_line = node.end_point[0] + 1
                sig_end = min(node.start_byte + 200, node.end_byte)
                sig_text = source[node.start_byte:sig_end].split('\n')[0]

                if node.type in class_types:
                    kind = 'class'
                elif node.type in ('method_definition', 'method_declaration'):
                    # JS/TS method_definition, Go/Java method_declaration
                    kind = 'method'
                elif node.parent and node.parent.type == 'block':
                    # Python: function inside class block
                    gp = node.parent.parent
                    if gp and gp.type in ('class_definition', 'class_body'):
                        kind = 'method'
                    else:
                        kind = 'function'
                elif node.parent and node.parent.type in ('class_body', 'impl_item',
                                                           'declaration_list',
                                                           'field_declaration_list'):
                    # JS/TS class_body, Rust impl_item, Java class body, C/C++ class body
                    kind = 'method'
                else:
                    kind = 'function'

                docstring = self._extract_docstring(node, source, language)

                results.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=start_line, end_line=end_line,
                    kind=kind, signature=sig_text, docstring=docstring,
                ))
            else:
                # Invalid name — still walk children (e.g. impl blocks contain methods)
                pass

        for child in node.children:
            self._walk_tree(child, source, file_path, language,
                           func_types, class_types, name_field, results)

    def _extract_docstring(self, node, source: str, language: str) -> str:
        """Extract docstring from a function/class AST node. Returns first 500 chars or ''."""
        if language == 'python':
            # Python: first child of body that is expression_statement containing a string
            body = node.child_by_field_name('body')
            if body and body.child_count > 0:
                first_stmt = body.children[0]
                if first_stmt.type == 'expression_statement' and first_stmt.child_count > 0:
                    expr = first_stmt.children[0]
                    if expr.type == 'string':
                        raw = source[expr.start_byte:expr.end_byte]
                        # Strip triple-quote delimiters
                        for delim in ('"""', "'''"):
                            if raw.startswith(delim) and raw.endswith(delim):
                                raw = raw[3:-3]
                                break
                        else:
                            # Single-quote string used as docstring
                            if (raw.startswith('"') and raw.endswith('"')) or \
                               (raw.startswith("'") and raw.endswith("'")):
                                raw = raw[1:-1]
                        return raw.strip()[:500]
        elif language in ('javascript', 'typescript'):
            # JSDoc: /** ... */ comment immediately before the node
            prev = node.prev_sibling
            if prev and prev.type == 'comment':
                text = source[prev.start_byte:prev.end_byte]
                if text.startswith('/**'):
                    text = text.lstrip('/').lstrip('*').rstrip('*').rstrip('/')
                    # Clean up JSDoc line prefixes
                    lines = [l.lstrip(' *') for l in text.split('\n')]
                    return '\n'.join(lines).strip()[:500]
        elif language == 'rust':
            # Rust: /// doc comments before the item
            doc_lines = []
            prev = node.prev_sibling
            while prev and prev.type == 'line_comment':
                text = source[prev.start_byte:prev.end_byte]
                if text.startswith('///'):
                    doc_lines.insert(0, text[3:].strip())
                else:
                    break
                prev = prev.prev_sibling
            if doc_lines:
                return '\n'.join(doc_lines).strip()[:500]
        elif language in ('java', 'c', 'cpp'):
            # Javadoc / Doxygen: /** ... */ before the node
            prev = node.prev_sibling
            if prev and prev.type in ('comment', 'block_comment'):
                text = source[prev.start_byte:prev.end_byte]
                if text.startswith('/**'):
                    text = text.lstrip('/').lstrip('*').rstrip('*').rstrip('/')
                    lines = [l.lstrip(' *') for l in text.split('\n')]
                    return '\n'.join(lines).strip()[:500]
        elif language == 'go':
            # Go: // comment lines before the function
            doc_lines = []
            prev = node.prev_sibling
            while prev and prev.type == 'comment':
                text = source[prev.start_byte:prev.end_byte]
                if text.startswith('//'):
                    doc_lines.insert(0, text[2:].strip())
                prev = prev.prev_sibling
            if doc_lines:
                return '\n'.join(doc_lines).strip()[:500]
        return ''

    def _fallback_parse(self, source: str, file_path: str, language: str) -> List[FunctionInfo]:
        """Regex-based fallback when tree-sitter isn't available."""
        import re
        functions = []
        lines = source.split('\n')

        if language == 'python':
            pattern = re.compile(r'^(\s*)(def|class)\s+(\w+)')

            for i, line in enumerate(lines):
                m = pattern.match(line)
                if m:
                    indent = len(m.group(1))
                    kind_str = m.group(2)
                    name = m.group(3)
                    kind = 'class' if kind_str == 'class' else 'function'

                    end_line = i + 1
                    for j in range(i + 1, len(lines)):
                        stripped = lines[j].strip()
                        if not stripped:
                            continue
                        line_indent = len(lines[j]) - len(lines[j].lstrip())
                        if line_indent <= indent:
                            break
                        end_line = j + 1

                    functions.append(FunctionInfo(
                        name=name, file_path=file_path, language=language,
                        start_line=i + 1, end_line=end_line,
                        kind=kind, signature=line.strip(),
                    ))
        elif language in ('javascript', 'typescript'):
            # Match: function declarations, class declarations, and const arrow functions
            func_pattern = re.compile(
                r'(?:export\s+)?(?:async\s+)?(?:function\s+(\w+)|class\s+(\w+))'
            )
            arrow_pattern = re.compile(
                r'(?:export\s+)?(?:const|let|var)\s+(\w+)\s*=\s*(?:async\s+)?(?:\([^)]*\)|[A-Za-z_]\w*)\s*=>'
            )
            for i, line in enumerate(lines):
                m = func_pattern.search(line)
                arrow_m = arrow_pattern.search(line) if not m else None
                if m:
                    func_name = m.group(1)
                    class_name = m.group(2)
                    name = func_name or class_name
                    kind = 'class' if class_name else 'function'
                elif arrow_m:
                    name = arrow_m.group(1)
                    kind = 'function'
                else:
                    continue

                end_line = self._find_brace_end(lines, i)
                functions.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=i + 1, end_line=end_line,
                    kind=kind, signature=line.strip(),
                ))

        elif language == 'rust':
            pattern = re.compile(
                r'(?:pub\s+)?(?:async\s+)?(?:unsafe\s+)?(?:(fn)\s+(\w+)|(struct|enum|trait|impl)\s+(\w+))'
            )
            for i, line in enumerate(lines):
                m = pattern.search(line)
                if not m:
                    continue
                if m.group(1):  # fn
                    name, kind = m.group(2), 'function'
                else:
                    name = m.group(4)
                    kind = 'class'
                end_line = self._find_brace_end(lines, i)
                functions.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=i + 1, end_line=end_line,
                    kind=kind, signature=line.strip(),
                ))

        elif language == 'go':
            pattern = re.compile(
                r'(?:func\s+(?:\(\w+\s+\*?\w+\)\s+)?(\w+)|type\s+(\w+)\s+(?:struct|interface))'
            )
            for i, line in enumerate(lines):
                m = pattern.search(line)
                if not m:
                    continue
                if m.group(1):
                    name, kind = m.group(1), 'function'
                else:
                    name, kind = m.group(2), 'class'
                end_line = self._find_brace_end(lines, i)
                functions.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=i + 1, end_line=end_line,
                    kind=kind, signature=line.strip(),
                ))

        elif language == 'java':
            _java_keywords = {'if', 'else', 'while', 'for', 'switch', 'catch',
                              'synchronized', 'return', 'throw', 'new', 'try'}
            class_pattern = re.compile(
                r'(?:public|private|protected)?\s*(?:static\s+)?(?:abstract\s+)?(?:class|interface|enum)\s+(\w+)'
            )
            method_pattern = re.compile(
                r'(?:public|private|protected)?\s*(?:static\s+)?(?:final\s+)?(?:\w+(?:<[^>]+>)?)\s+(\w+)\s*\('
            )
            for i, line in enumerate(lines):
                mc = class_pattern.search(line)
                mm = method_pattern.search(line) if not mc else None
                if mc:
                    name, kind = mc.group(1), 'class'
                elif mm:
                    name = mm.group(1)
                    if name in _java_keywords:
                        continue
                    kind = 'function'
                else:
                    continue
                end_line = self._find_brace_end(lines, i)
                functions.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=i + 1, end_line=end_line,
                    kind=kind, signature=line.strip(),
                ))

        elif language in ('c', 'cpp'):
            struct_pattern = re.compile(
                r'(?:typedef\s+)?(?:struct|class|enum)\s+(\w+)'
            )
            func_pattern = re.compile(
                r'(?:\w[\w\s\*]*?)\s+(\w+)\s*\([^)]*\)\s*\{'
            )
            for i, line in enumerate(lines):
                ms = struct_pattern.search(line)
                mf = func_pattern.search(line) if not ms else None
                if ms:
                    name, kind = ms.group(1), 'class'
                elif mf:
                    name, kind = mf.group(1), 'function'
                else:
                    continue
                end_line = self._find_brace_end(lines, i)
                functions.append(FunctionInfo(
                    name=name, file_path=file_path, language=language,
                    start_line=i + 1, end_line=end_line,
                    kind=kind, signature=line.strip(),
                ))

        return functions

    @staticmethod
    def _strip_strings_and_comments(line: str) -> str:
        """Remove string literals and single-line comments so brace counting is accurate."""
        # Strip single-line comments (// for C-like languages)
        result = re.sub(r'//.*$', '', line)
        # Strip string literals (double and single quoted, handling escaped quotes)
        result = re.sub(r'"(?:[^"\\]|\\.)*"', '""', result)
        result = re.sub(r"'(?:[^'\\]|\\.)*'", "''", result)
        # Strip backtick template literals (JS/TS)
        result = re.sub(r'`(?:[^`\\]|\\.)*`', '``', result)
        return result

    @staticmethod
    def _find_brace_end(lines: List[str], start: int) -> int:
        """Find the end of a brace-delimited block starting at the given line."""
        depth = 0
        found_brace = False
        max_scan = min(start + 1000, len(lines))
        for j in range(start, max_scan):
            clean = ASTParser._strip_strings_and_comments(lines[j])
            depth += clean.count('{') - clean.count('}')
            if depth > 0:
                found_brace = True
            if found_brace and depth <= 0:
                return j + 1
        return max_scan


class BlameAnalyzer:
    """Maps git blame data to function boundaries."""

    def analyze_function_blame(self, blame_data: List[tuple],
                                function: FunctionInfo) -> List[BlameEntry]:
        """Calculate ownership percentages for a function from blame data.

        Args:
            blame_data: List of (commit_hash, author, line_number) tuples
            function: FunctionInfo with line boundaries
        """
        author_lines: Dict[str, Dict] = defaultdict(lambda: {'count': 0, 'commits': defaultdict(int)})

        for commit_hash, author, line_num in blame_data:
            if function.start_line <= line_num <= function.end_line:
                author_lines[author]['count'] += 1
                author_lines[author]['commits'][commit_hash] += 1

        # Use actual blamed lines as denominator so percentages sum to 100%
        total_blamed = sum(d['count'] for d in author_lines.values())
        if total_blamed == 0:
            return []

        entries = []
        for author, data in author_lines.items():
            most_common_commit = max(data['commits'], key=data['commits'].get)
            entries.append(BlameEntry(
                function_name=function.name,
                file_path=function.file_path,
                author=author,
                line_count=data['count'],
                percentage=round(data['count'] / total_blamed * 100, 1),
                commit_hash=most_common_commit,
            ))

        entries.sort(key=lambda e: e.percentage, reverse=True)
        return entries
