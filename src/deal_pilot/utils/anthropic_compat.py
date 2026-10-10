"""Compatibility fixes for Anthropic-compatible providers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel


def patch_nullable_cache_creation_usage() -> None:
    """Treat nullable cache TTL counters as zero in langchain-anthropic.

    Some Anthropic-compatible providers return both cache TTL keys with null
    values. langchain-anthropic 1.7.1 adds those values as integers and raises
    ``TypeError`` before it can return the model response.
    """
    import langchain_anthropic.chat_models as chat_models

    original = chat_models._create_usage_metadata
    if getattr(original, "_llm_ops_nullable_cache_fix", False):
        return

    def create_usage_metadata(anthropic_usage: BaseModel) -> Any:
        cache_creation = getattr(anthropic_usage, "cache_creation", None)
        if cache_creation:
            if isinstance(cache_creation, BaseModel):
                values = cache_creation.model_dump()
                values.update({key: value or 0 for key, value in values.items()})
                cache_creation = cache_creation.model_copy(update=values)
            elif isinstance(cache_creation, Mapping):
                cache_creation = {
                    key: value or 0 for key, value in cache_creation.items()
                }
            anthropic_usage = anthropic_usage.model_copy(
                update={"cache_creation": cache_creation}
            )
        return original(anthropic_usage)

    create_usage_metadata._llm_ops_nullable_cache_fix = True  # type: ignore[attr-defined]
    chat_models._create_usage_metadata = create_usage_metadata
