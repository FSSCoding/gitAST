"""Dependency file parser for tracking additions, removals, and version bumps."""
import json
import re
from typing import Dict, List, Optional, Tuple

DEP_FILES = {
    'requirements.txt', 'requirements-dev.txt', 'requirements-test.txt',
    'package.json', 'pyproject.toml', 'Cargo.toml', 'go.mod',
}


def is_dep_file(file_path: str) -> bool:
    """Check if a file path is a supported dependency file."""
    basename = file_path.rsplit('/', 1)[-1] if '/' in file_path else file_path
    if basename in DEP_FILES:
        return True
    if basename.startswith('requirements') and basename.endswith('.txt'):
        return True
    return False


def parse_deps(content: str, file_path: str) -> Dict[str, str]:
    """Parse a dependency file into {package_name: version_spec} dict."""
    basename = file_path.rsplit('/', 1)[-1] if '/' in file_path else file_path

    if basename.startswith('requirements') and basename.endswith('.txt'):
        return _parse_requirements_txt(content)
    elif basename == 'package.json':
        return _parse_package_json(content)
    elif basename == 'pyproject.toml':
        return _parse_pyproject_toml(content)
    elif basename == 'Cargo.toml':
        return _parse_cargo_toml(content)
    elif basename == 'go.mod':
        return _parse_go_mod(content)
    return {}


def diff_deps(before: Dict[str, str], after: Dict[str, str]) -> List[Tuple[str, str, Optional[str], Optional[str]]]:
    """Diff two dependency dicts. Returns list of (package, change_type, old_version, new_version).

    change_type: 'added', 'removed', 'bumped'
    """
    changes = []
    all_pkgs = set(before.keys()) | set(after.keys())

    for pkg in sorted(all_pkgs):
        old = before.get(pkg)
        new = after.get(pkg)

        if old is None and new is not None:
            changes.append((pkg, 'added', None, new))
        elif old is not None and new is None:
            changes.append((pkg, 'removed', old, None))
        elif old != new:
            changes.append((pkg, 'bumped', old, new))

    return changes


def _parse_requirements_txt(content: str) -> Dict[str, str]:
    """Parse requirements.txt format."""
    deps = {}
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('-'):
            continue
        # Skip URL-based deps (git+, http+, etc.)
        if re.match(r'^(git\+|https?://|ssh://)', line):
            continue
        # Strip inline comments
        line = re.sub(r'\s+#.*$', '', line)
        # Handle: package[extras]==1.0, package>=1.0,<3.0, package~=1.0, package
        match = re.match(r'^([a-zA-Z0-9._-]+)(?:\[.*?\])?\s*((?:[><=!~]+\s*\S+(?:\s*,\s*[><=!~]+\s*\S+)*)?)$', line)
        if match:
            name = match.group(1).lower().replace('_', '-')
            version = match.group(2).strip() or '*'
            deps[name] = version
    return deps


def _parse_package_json(content: str) -> Dict[str, str]:
    """Parse package.json dependencies."""
    try:
        data = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        return {}

    deps = {}
    for section in ('dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies'):
        section_deps = data.get(section, {})
        if isinstance(section_deps, dict):
            for name, version in section_deps.items():
                deps[name] = str(version)
    return deps


def _parse_pyproject_toml(content: str) -> Dict[str, str]:
    """Parse pyproject.toml dependencies."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            return {}

    try:
        data = tomllib.loads(content)
    except Exception:
        return {}

    deps = {}
    # PEP 621: [project.dependencies]
    for dep_str in data.get('project', {}).get('dependencies', []):
        name, version = _parse_pep508(dep_str)
        if name:
            deps[name] = version

    # PEP 621: [project.optional-dependencies]
    for group_deps in data.get('project', {}).get('optional-dependencies', {}).values():
        for dep_str in group_deps:
            name, version = _parse_pep508(dep_str)
            if name:
                deps[name] = version

    # Poetry: [tool.poetry.dependencies]
    for section in ('dependencies', 'dev-dependencies'):
        poetry_deps = data.get('tool', {}).get('poetry', {}).get(section, {})
        if isinstance(poetry_deps, dict):
            for name, spec in poetry_deps.items():
                if name == 'python':
                    continue
                if isinstance(spec, str):
                    deps[name.lower()] = spec
                elif isinstance(spec, dict):
                    deps[name.lower()] = spec.get('version', '*')

    return deps


def _parse_pep508(dep_str: str) -> Tuple[str, str]:
    """Parse a PEP 508 dependency string like 'requests>=2.0'."""
    match = re.match(r'^([a-zA-Z0-9._-]+)\s*(\[.*?\])?\s*([><=!~].*)?', dep_str)
    if match:
        name = match.group(1).lower().replace('_', '-')
        version = (match.group(3) or '*').strip()
        return name, version
    return '', '*'


def _parse_cargo_toml(content: str) -> Dict[str, str]:
    """Parse Cargo.toml dependencies."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            return {}

    try:
        data = tomllib.loads(content)
    except Exception:
        return {}

    deps = {}
    for section in ('dependencies', 'dev-dependencies', 'build-dependencies'):
        section_deps = data.get(section, {})
        if isinstance(section_deps, dict):
            for name, spec in section_deps.items():
                if isinstance(spec, str):
                    deps[name] = spec
                elif isinstance(spec, dict):
                    deps[name] = spec.get('version', '*')
    return deps


def _parse_go_mod(content: str) -> Dict[str, str]:
    """Parse go.mod require blocks."""
    deps = {}
    in_require = False

    for line in content.splitlines():
        line = line.strip()

        if line.startswith('require ('):
            in_require = True
            continue
        if in_require and line == ')':
            in_require = False
            continue

        # Strip inline comments
        if '//' in line:
            line = line[:line.index('//')].strip()

        # Single-line require
        if line.startswith('require ') and '(' not in line:
            parts = line[8:].strip().split()
            if len(parts) >= 2:
                deps[parts[0]] = parts[1]
            continue

        # Inside require block
        if in_require and line:
            parts = line.split()
            if len(parts) >= 2:
                deps[parts[0]] = parts[1]

    return deps
