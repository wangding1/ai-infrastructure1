from typing import ClassVar

from pydantic_settings import SettingsConfigDict

from deal_pilot.config.base_settings import BaseSettingsWithEnv


class MinioSettings(BaseSettingsWithEnv):
    # MinIO 访问密钥。
    access_key: str = ""
    # MinIO 秘密密钥。
    secret_key: str = ""
    # MinIO API 地址，不包含协议前缀。
    endpoint: str = "localhost:9002"
    # 是否使用 HTTPS 连接。
    secure: bool = False
    # 沙箱内上传时使用的 API 地址（容器访问宿主机 MinIO）。
    endpoint_in_sandbox: str = ""
    # Agent 产物使用的桶名。
    bucket: str = "deal-pilot"
    # 对外访问 URL 前缀（含协议，可含桶路径），末尾不带 /。
    # 入口地址为 {url_prefix}/{user_id}/{target_dirname}/{entry_file}；
    # 未配置时根据 endpoint、bucket 与对象键自动生成。
    url_prefix: str = ""

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_prefix="MINIO_"
    )


minio_settings = MinioSettings()
