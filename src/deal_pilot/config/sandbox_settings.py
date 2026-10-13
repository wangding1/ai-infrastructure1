from typing import ClassVar

from pydantic_settings import SettingsConfigDict

from deal_pilot.config.base_settings import BaseSettingsWithEnv


class SandboxSettings(BaseSettingsWithEnv):
    api_key: str = ""
    domain: str = ""
    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="OPEN_SANDBOX_"
    )


sandbox_settings = SandboxSettings()
