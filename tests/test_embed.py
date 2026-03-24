"""Tests for GitAST semantic search: embed.py, DataStore embedding methods, hybrid search."""
import os
import sqlite3
import tempfile
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

from gitast.embed import (
    EmbeddingClient, classify_query,
    prepare_function_text, prepare_commit_text,
)
from gitast.core import DataStore


# --- classify_query ---

class TestClassifyQuery:
    def test_camelcase_identifier(self):
        fts5_w, sem_w = classify_query("EmailSyncManager")
        assert fts5_w > 0.7, "CamelCase identifier should strongly favor FTS5"
        assert abs(fts5_w + sem_w - 1.0) < 0.001

    def test_snake_case_identifier(self):
        fts5_w, sem_w = classify_query("get_auth_token")
        assert fts5_w > 0.7, "snake_case identifier should strongly favor FTS5"

    def test_conceptual_multi_word(self):
        fts5_w, sem_w = classify_query("email streaming bridge")
        assert sem_w > 0.6, "Multi-word conceptual query should favor semantic"

    def test_question_query(self):
        fts5_w, sem_w = classify_query("how does authentication work")
        assert sem_w > 0.7, "Question-style query should strongly favor semantic"

    def test_single_conceptual_word(self):
        fts5_w, sem_w = classify_query("authentication")
        assert sem_w >= 0.5, "Single conceptual word should lean semantic"

    def test_balanced_default(self):
        fts5_w, sem_w = classify_query("x")
        assert abs(fts5_w + sem_w - 1.0) < 0.001
        # Single short token with no patterns — should be balanced
        assert 0.15 <= fts5_w <= 0.85

    def test_weights_sum_to_one(self):
        for q in ["test", "EmailSync", "how does this work", "get_data", "memory management"]:
            fts5_w, sem_w = classify_query(q)
            assert abs(fts5_w + sem_w - 1.0) < 0.001, f"Weights don't sum to 1 for {q!r}"

    def test_weights_clamped(self):
        for q in ["EmailSyncManager", "how does authentication work in this system"]:
            fts5_w, sem_w = classify_query(q)
            assert fts5_w >= 0.15
            assert sem_w >= 0.15


# --- Text preparation ---

class TestTextPreparation:
    def test_prepare_function_text_full(self):
        def split_ids(name):
            return "Email Sync Manager"
        text = prepare_function_text(
            "EmailSyncManager", "class", "src/email/sync.py",
            "class EmailSyncManager(BaseManager)", "Manages email sync.",
            split_ids
        )
        assert "EmailSyncManager" in text
        assert "class in src/email/sync.py" in text
        assert "Signature:" in text
        assert "Docstring: Manages email sync." in text
        assert "Keywords:" in text

    def test_prepare_function_text_no_docstring(self):
        text = prepare_function_text(
            "foo", "function", "test.py", "def foo()", "",
            lambda x: x
        )
        assert "Docstring:" not in text

    def test_prepare_function_text_no_keyword_expansion(self):
        text = prepare_function_text(
            "x", "function", "test.py", "def x()", "",
            lambda name: name  # no expansion
        )
        assert "Keywords:" not in text

    def test_prepare_commit_text(self):
        text = prepare_commit_text("Fix memory leak", 4, "bob")
        assert "Fix memory leak" in text
        assert "Files changed: 4" in text
        assert "Author: bob" in text


# --- DataStore embedding methods ---

class TestDataStoreEmbeddings:
    @pytest.fixture
    def store(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        s = DataStore(db_path)
        s.connect()
        s.create_schema()
        return s

    def test_has_embeddings_empty(self, store):
        assert store.has_embeddings() is False

    def test_save_and_retrieve_embedding(self, store):
        vec = np.random.randn(384).astype(np.float32)
        store.save_embedding('function', 'foo::bar.py', 'test text', vec.tobytes(), 'minilm')
        store.conn.commit()
        assert store.has_embeddings() is True
        ids = store.get_embedded_ref_ids('function')
        assert 'foo::bar.py' in ids

    def test_save_embeddings_batch(self, store):
        entries = []
        for i in range(10):
            vec = np.random.randn(384).astype(np.float32)
            entries.append(('function', f'func{i}::file.py', f'text{i}', vec.tobytes(), 'minilm'))
        store.save_embeddings_batch(entries)
        assert len(store.get_embedded_ref_ids('function')) == 10

    def test_clear_embeddings(self, store):
        vec = np.random.randn(384).astype(np.float32)
        store.save_embedding('function', 'foo::bar.py', 'test', vec.tobytes(), 'minilm')
        store.conn.commit()
        assert store.has_embeddings()
        store.clear_embeddings()
        assert not store.has_embeddings()

    def test_get_all_embeddings(self, store):
        vecs = np.random.randn(5, 384).astype(np.float32)
        for i in range(5):
            store.save_embedding('function', f'f{i}::x.py', f't{i}', vecs[i].tobytes(), 'minilm')
        store.conn.commit()
        meta, matrix = store.get_all_embeddings()
        assert len(meta) == 5
        assert matrix.shape == (5, 384)

    def test_get_embedding_stats(self, store):
        store.set_meta('embed_model', 'minilm')
        store.set_meta('embed_dim', '384')
        vec = np.random.randn(384).astype(np.float32)
        store.save_embedding('function', 'f::x.py', 't', vec.tobytes(), 'minilm')
        store.save_embedding('commit', 'abc123', 't', vec.tobytes(), 'minilm')
        store.conn.commit()
        stats = store.get_embedding_stats()
        assert stats['total'] == 2
        assert stats['functions'] == 1
        assert stats['commits'] == 1
        assert stats['model'] == 'minilm'

    def test_semantic_search(self, store):
        # Create some embeddings with known similarity
        target = np.random.randn(384).astype(np.float32)
        target /= np.linalg.norm(target)
        # Similar vector
        similar = target + np.random.randn(384).astype(np.float32) * 0.1
        similar /= np.linalg.norm(similar)
        # Dissimilar vector
        dissimilar = np.random.randn(384).astype(np.float32)
        dissimilar /= np.linalg.norm(dissimilar)

        store.save_embedding('function', 'similar::a.py', 'similar', similar.tobytes(), 'minilm')
        store.save_embedding('function', 'dissimilar::b.py', 'dissimilar', dissimilar.tobytes(), 'minilm')
        store.conn.commit()

        results = store.semantic_search(target, limit=10)
        assert len(results) >= 1
        # Similar should be first (enriched results use 'name' not 'ref_id')
        assert results[0]['name'] == 'similar'
        assert results[0]['file_path'] == 'a.py'
        assert results[0]['cosine'] > 0.3  # should be clearly more similar than random
        assert results[0]['relevance'] in ('HIGH', 'GOOD', 'FAIR', 'LOW', 'WEAK')

    def test_hybrid_search_fts5_only(self, store):
        """Hybrid search works when no embeddings exist (FTS5 fallback)."""
        # Add some data for FTS5
        from gitast.models import GitCommit
        from datetime import datetime
        store.save_commit(GitCommit(
            hash='abc123def456', author='bob',
            timestamp=datetime.now(), message='Fix authentication bug',
            files_changed=2,
        ))
        store.flush()
        store.rebuild_search_index()

        results = store.hybrid_search('authentication', None, limit=10)
        assert len(results) >= 1
        assert results[0]['source'] in ('exact', 'hybrid')

    def test_hybrid_search_with_embeddings(self, store):
        """Hybrid search merges FTS5 and semantic results."""
        from gitast.models import GitCommit
        from datetime import datetime

        # Add commit for FTS5
        store.save_commit(GitCommit(
            hash='abc123def456', author='bob',
            timestamp=datetime.now(), message='Fix authentication token refresh',
            files_changed=2,
        ))
        store.flush()
        store.rebuild_search_index()

        # Add embedding for the same commit
        query_vec = np.random.randn(384).astype(np.float32)
        query_vec /= np.linalg.norm(query_vec)
        commit_vec = query_vec + np.random.randn(384).astype(np.float32) * 0.05
        commit_vec /= np.linalg.norm(commit_vec)
        store.save_embedding('commit', 'abc123def456', 'Fix auth', commit_vec.tobytes(), 'minilm')
        store.conn.commit()

        results = store.hybrid_search('authentication', query_vec, limit=10)
        assert len(results) >= 1

    def test_hybrid_search_relevance_labels(self, store):
        """Results include relevance labels."""
        from gitast.models import GitCommit
        from datetime import datetime
        store.save_commit(GitCommit(
            hash='abc123def456', author='bob',
            timestamp=datetime.now(), message='Fix bug',
            files_changed=1,
        ))
        store.flush()
        store.rebuild_search_index()
        results = store.hybrid_search('bug', None, limit=10)
        for r in results:
            assert 'relevance' in r
            assert r['relevance'] in ('HIGH', 'GOOD', 'FAIR', 'LOW', 'WEAK')

    def test_hybrid_search_diversity(self, store):
        """Max 3 results per file."""
        from gitast.models import FunctionInfo
        for i in range(10):
            store.save_function(FunctionInfo(
                name=f'func{i}', file_path='same_file.py', language='python',
                start_line=i*10+1, end_line=i*10+10, kind='function',
                signature=f'def func{i}()', docstring='test function',
            ))
        store.flush()
        store.rebuild_search_index()
        results = store.hybrid_search('func', None, limit=20)
        same_file_count = sum(1 for r in results if r.get('file_path') == 'same_file.py')
        assert same_file_count <= 3


# --- EmbeddingClient ---

class TestEmbeddingClient:
    def test_health_check_no_endpoint(self):
        client = EmbeddingClient(model="nonexistent-model")
        # Should return None when no endpoint is reachable
        result = client.health_check()
        # Can't guarantee it'll fail (LM Studio might be running), just check it returns str or None
        assert result is None or isinstance(result, str)

    def test_embed_batch_returns_normalized(self):
        """If embedding works, vectors should be L2-normalized."""
        client = EmbeddingClient()
        if client.health_check() is None:
            pytest.skip("No embedding endpoint available")
        result = client.embed_batch(["test query"])
        assert result is not None
        norms = np.linalg.norm(result, axis=1)
        np.testing.assert_allclose(norms, 1.0, atol=0.01)


# --- Docstring extraction ---

class TestDocstringExtraction:
    def test_python_docstring(self):
        from gitast.analysis import ASTParser
        parser = ASTParser()
        source = '''
def hello():
    """This is a docstring."""
    pass

class Foo:
    """Class docstring here."""
    pass
'''
        results = parser.parse_file(source, 'test.py', 'python')
        assert len(results) >= 2
        hello = next(r for r in results if r.name == 'hello')
        assert hello.docstring == "This is a docstring."
        foo = next(r for r in results if r.name == 'Foo')
        assert foo.docstring == "Class docstring here."

    def test_python_no_docstring(self):
        from gitast.analysis import ASTParser
        parser = ASTParser()
        source = '''
def bare():
    x = 1
    return x
'''
        results = parser.parse_file(source, 'test.py', 'python')
        bare = next(r for r in results if r.name == 'bare')
        assert bare.docstring == ""

    def test_python_multiline_docstring(self):
        from gitast.analysis import ASTParser
        parser = ASTParser()
        source = '''
def multi():
    """
    This is a
    multiline docstring.
    """
    pass
'''
        results = parser.parse_file(source, 'test.py', 'python')
        multi = next(r for r in results if r.name == 'multi')
        assert "multiline docstring" in multi.docstring

    def test_docstring_truncation(self):
        from gitast.analysis import ASTParser
        parser = ASTParser()
        long_doc = "x" * 600
        source = f'''
def long_doc():
    """{long_doc}"""
    pass
'''
        results = parser.parse_file(source, 'test.py', 'python')
        func = next(r for r in results if r.name == 'long_doc')
        assert len(func.docstring) <= 500
