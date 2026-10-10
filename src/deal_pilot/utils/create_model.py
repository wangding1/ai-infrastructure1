from langchain.chat_models import init_chat_model
from deal_pilot.config.anthropic_settings import anthropic_settings
from deal_pilot.utils.anthropic_compat import patch_nullable_cache_creation_usage


def create_model(**kwargs):
    patch_nullable_cache_creation_usage()
    config = {
        "model": anthropic_settings.default_model,
        "thinking": {"type": "disabled"},
        "model_provider": "anthropic",
    }
    config.update(kwargs)

    model = init_chat_model(**config)
    return model
