"""Embedding client and search utilities for GitAST semantic search."""
import re
from typing import List, Optional, Tuple

import numpy as np

from .llm import LLMConfig


# Patterns to identify embedding models from /v1/models, in preference order.
# First match wins — put preferred models first.
_EMBEDDING_PREFER = [
    'minilm',            # small, fast, good for code
    'nomic',             # nomic-embed-text, solid general purpose
    'bge',               # BAAI general embedding
    'granite-embedding', # IBM granite
]
_EMBEDDING_HINTS = _EMBEDDING_PREFER + ['embed']  # broader catch-all last


class EmbeddingClient:
    """OpenAI-compatible embedding client with endpoint fallback and model auto-detection."""

    def __init__(self, config: Optional[LLMConfig] = None,
                 model: Optional[str] = None):
        self.config = config or LLMConfig()
        self._requested_model = model  # None = auto-detect
        self.model: Optional[str] = model
        self._endpoint: Optional[str] = None
        self._dim: Optional[int] = None

    def _get_endpoints(self) -> List[str]:
        endpoints = [self.config.endpoint, self.config.fallback_endpoint]
        if self.config.use_remote:
            endpoints.append(self.config.remote_endpoint)
        return endpoints

    @staticmethod
    def _detect_embedding_models(client) -> List[str]:
        """Query /v1/models and return candidate embedding models, best first."""
        try:
            models = client.with_options(timeout=5.0).models.list()
        except Exception:
            return []

        # Score each model by preference order
        candidates = []
        for m in models.data:
            mid = m.id.lower()
            # Must contain 'embed' somewhere to be an embedding model
            if 'embed' not in mid:
                continue
            # Skip multimodal/VL models — they often can't do plain text embeddings
            if '-vl-' in mid or '-vl.' in mid:
                continue
            # Score by preference (lower = better)
            score = len(_EMBEDDING_PREFER)  # default: lowest priority
            for i, hint in enumerate(_EMBEDDING_PREFER):
                if hint in mid:
                    score = i
                    break
            candidates.append((score, m.id))

        candidates.sort()
        return [c[1] for c in candidates]

    def health_check(self) -> Optional[str]:
        """Find first endpoint with a working embedding model.

        If no model was specified, auto-detects from /v1/models (tries best first).
        Returns endpoint URL or None.
        """
        try:
            from openai import OpenAI
        except ImportError:
            return None

        for endpoint in self._get_endpoints():
            try:
                client = OpenAI(base_url=endpoint, api_key="not-needed")

                if self._requested_model:
                    models_to_try = [self._requested_model]
                else:
                    models_to_try = self._detect_embedding_models(client)
                    if not models_to_try:
                        continue

                for model in models_to_try:
                    try:
                        resp = client.with_options(timeout=10.0).embeddings.create(
                            model=model, input=["test"]
                        )
                        self._endpoint = endpoint
                        self.model = model
                        self._dim = len(resp.data[0].embedding)
                        return endpoint
                    except Exception:
                        continue
            except Exception:
                continue
        return None

    @property
    def dim(self) -> Optional[int]:
        return self._dim

    def embed_batch(self, texts: List[str]) -> Optional[np.ndarray]:
        """Embed a batch of texts. Returns (N, dim) L2-normalized float32 array or None."""
        try:
            from openai import OpenAI
        except ImportError:
            return None

        if not self._endpoint:
            if not self.health_check():
                return None

        try:
            client = OpenAI(base_url=self._endpoint, api_key="not-needed")
            resp = client.with_options(timeout=60.0).embeddings.create(
                model=self.model, input=texts
            )
            vectors = np.array([d.embedding for d in resp.data], dtype=np.float32)
            # L2 normalize so dot product = cosine similarity
            norms = np.linalg.norm(vectors, axis=1, keepdims=True)
            norms[norms == 0] = 1
            vectors = vectors / norms
            self._dim = vectors.shape[1]
            return vectors
        except Exception:
            return None

    def embed_single(self, text: str) -> Optional[np.ndarray]:
        """Embed a single text. Returns (dim,) L2-normalized float32 array or None."""
        result = self.embed_batch([text])
        if result is not None:
            return result[0]
        return None


# --- Text preparation ---

def prepare_function_text(name: str, kind: str, file_path: str,
                          signature: str, docstring: str,
                          split_identifiers_fn) -> str:
    """Build embedding text for a function entry."""
    parts = [f"{name} — {kind} in {file_path}"]
    if signature:
        parts.append(f"Signature: {signature}")
    if docstring:
        parts.append(f"Docstring: {docstring}")
    keywords = split_identifiers_fn(name)
    if keywords != name:
        parts.append(f"Keywords: {keywords}")
    return '\n'.join(parts)


def prepare_commit_text(message: str, files_changed: int, author: str) -> str:
    """Build embedding text for a commit entry."""
    parts = [message]
    parts.append(f"Files changed: {files_changed}")
    parts.append(f"Author: {author}")
    return '\n'.join(parts)


# --- Query classification ---

# Patterns that indicate technical/identifier queries (favor FTS5)
_TECHNICAL_PATTERNS = [
    re.compile(r'[A-Z][a-z]+[A-Z]'),         # CamelCase
    re.compile(r'[a-z]+_[a-z]+'),             # snake_case
    re.compile(r'[A-Z]{2,}'),                 # CONSTANTS
    re.compile(r'\w+\.\w+'),                  # dotted.path
    re.compile(r'\w+\(\)'),                   # func()
    re.compile(r'\*\.\w+'),                   # *.ext
]

# Patterns that indicate conceptual/natural language queries (favor semantic)
_CONCEPTUAL_PATTERNS = [
    re.compile(r'\b(?:how|what|where|when|why|which)\b', re.I),
    re.compile(r'\b(?:does|is|are|was|were|can|should)\b', re.I),
    re.compile(r'\b(?:architecture|design|pattern|flow|process)\b', re.I),
    re.compile(r'\b(?:management|handling|processing|streaming)\b', re.I),
]


def classify_query(query: str) -> Tuple[float, float]:
    """Classify query as technical vs conceptual.

    Returns (fts5_weight, semantic_weight) summing to 1.0.
    """
    tech_score = 0
    concept_score = 0

    for pattern in _TECHNICAL_PATTERNS:
        if pattern.search(query):
            tech_score += 1

    for pattern in _CONCEPTUAL_PATTERNS:
        if pattern.search(query):
            concept_score += 1

    # Single token that looks like an identifier — strong FTS5
    tokens = query.strip().split()
    if len(tokens) == 1 and re.match(r'^[A-Za-z_]\w*$', tokens[0]):
        if any(c.isupper() for c in tokens[0][1:]):  # CamelCase
            tech_score += 3
        elif '_' in tokens[0]:  # snake_case
            tech_score += 3
        else:
            concept_score += 1  # single word like "authentication"

    # Multi-word with no technical patterns — conceptual
    if len(tokens) >= 3 and tech_score == 0:
        concept_score += 2

    # Convert to weights
    total = tech_score + concept_score
    if total == 0:
        return 0.5, 0.5

    fts5_w = tech_score / total
    semantic_w = concept_score / total

    # Clamp to reasonable range (never fully exclude either method)
    fts5_w = max(0.15, min(0.85, fts5_w))
    semantic_w = 1.0 - fts5_w

    return fts5_w, semantic_w
