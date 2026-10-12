import time
import asyncio
import httpx
from deal_pilot.config.nacos_settings import nacos_settings

_access_token: str | None = None
_expires_at = 0
_token_lock = asyncio.Lock()


async def get_access_token() -> str:
    "调用该函数，获取 access_token"
    global _access_token, _expires_at
    if _access_token and time.monotonic() < _expires_at:
        return _access_token

    # 同一时刻只能有一个协程进入锁获取token
    async with _token_lock:
        if _access_token and time.monotonic() < _expires_at:
            return _access_token

        # 重新获取 access_token
        start_at = time.monotonic()
        async with httpx.AsyncClient(
            base_url=nacos_settings.server_address, timeout=10
        ) as client:
            response = await client.post(
                "/nacos/v3/auth/user/login",
                data={
                    "username": nacos_settings.username,
                    "password": nacos_settings.password,
                },
            )
            response.raise_for_status()
            login_result = response.json()

        token = login_result["accessToken"]
        ttl = float(login_result["tokenTtl"])
        _access_token = token
        _expires_at = start_at + ttl - 30
        return token
