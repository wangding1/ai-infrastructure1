from deal_pilot.config.base_settings import BaseSettingsWithEnv


class AnthropicSettings(BaseSettingsWithEnv):
    api_key: str = ""
    base_url: str = ""
    default_model: str = ""

    model_config = {"env_prefix": "ANTHROPIC_"}


anthropic_settings = AnthropicSettings()
