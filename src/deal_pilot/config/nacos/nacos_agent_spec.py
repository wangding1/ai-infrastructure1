import asyncio
import json

import httpx

from deal_pilot.config.nacos.nacos_access_token import get_access_token
from deal_pilot.config.nacos.nacos_mcp import get_mcps
from deal_pilot.config.nacos.nacos_schemas import AgentSpec
from deal_pilot.config.nacos_settings import nacos_settings
from deal_pilot.utils.func_tools import func_cache


@func_cache(ttl=nacos_settings.ttl, lazy=True)
async def get_agent_spec(agent_name: str) -> AgentSpec:
    access_token = await get_access_token()
    async with httpx.AsyncClient(
        base_url=nacos_settings.server_address, headers={"accessToken": access_token}
    ) as client:
        response = await client.get(
            "/nacos/v3/client/ai/agentspecs",
            params={
                "namespaceId": nacos_settings.namespace_id,
                "name": agent_name,
                "label": "production",
            },
        )
        response = response.json()
    if response["code"] != 0:
        return AgentSpec(name=agent_name)

    response = response["data"]
    config = response["content"]
    config = json.loads(config)
    if "mcps" in config:
        config["mcps"] = await get_mcps(config["mcps"])

    system_prompt = (
        response.get("resource", {})
        .get(f"instruction_{agent_name}/AGENTS__MD", {})
        .get("content", "")
    )

    config["system_prompt"] = system_prompt
    config["name"] = agent_name
    return AgentSpec.model_validate(config)


if __name__ == "__main__":

    async def main():
        await get_agent_spec("main_agent")
        await get_agent_spec("main_agent")
        await get_agent_spec("main_agent")
        await get_agent_spec("web_agent")
        await get_agent_spec("web_agent")
        await get_agent_spec("web_agent")
        await asyncio.sleep(5)
        result = await get_agent_spec("main_agent")
        print(result)
        result = await get_agent_spec("web_agent")
        print(result)
        await asyncio.sleep(5)

    asyncio.run(main())
