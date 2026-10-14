import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from importlib import import_module
from typing import Any

from deepagents import create_deep_agent
from deepagents.backends.protocol import BackendProtocol
from deepagents.middleware import CompiledSubAgent
from langchain.agents.middleware.types import AgentMiddleware
from langchain.tools import BaseTool
from langchain_core.runnables import Runnable
from langchain_mcp_adapters.client import MultiServerMCPClient

from deal_pilot.backends.sandbox_backend import SandboxBackend
from deal_pilot.config.nacos.nacos_agent_spec import get_agent_spec
from deal_pilot.config.nacos.nacos_schemas import AgentSpec
from deal_pilot.config.nacos.nacos_skill import agent_skills_root
from deal_pilot.tools import get_tools
from deal_pilot.utils.create_model import create_model
from deal_pilot.utils.func_tools import func_cache

_AGENT_FACTORIES = {"web_agent": "deal_pilot.agents.web"}


class AgentFactory(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...

    agent_config: AgentSpec | None = None

    def _get_model(self) -> Any:
        if not self.agent_config:
            return create_model()
        return create_model(self.agent_config.model)

    @func_cache(ttl=86400)
    async def _get_mcp_tools(self, mcp_name: str) -> Sequence[BaseTool]:
        if not self.agent_config:
            return []
        if mcp_name not in self.agent_config.mcps:
            return []
        mcp_config = self.agent_config.mcps[mcp_name]
        client = MultiServerMCPClient(
            connections={
                mcp_name: {
                    "transport": "streamable_http",
                    "url": mcp_config.url,
                }
            },
            tool_name_prefix=True,
        )
        mcp_tools = await client.get_tools()

        disabled_tool_names = [
            f"{mcp_name}_{name}" for name in mcp_config.disabled_tools
        ]
        tools = [tool for tool in mcp_tools if tool.name not in disabled_tool_names]
        return tools

    async def _get_tools(self) -> Sequence[BaseTool]:
        if not self.agent_config:
            return []
        # 获取内置工具
        tools = get_tools(self.agent_config.tools)
        mcp_tool_groups = await asyncio.gather(
            *[
                self._get_mcp_tools(mcp_name)
                for mcp_name in self.agent_config.mcps
            ]
        )
        for mcp_tools in mcp_tool_groups:
            tools.extend(mcp_tools)
        return tools

    async def _get_subagents(self) -> list[dict]:
        if not self.agent_config:
            return []
        specs = await asyncio.gather(
            *[get_agent_spec(agent_name) for agent_name in self.agent_config.subagents]
        )

        subagents = []
        for spec in specs:
            name = spec.name
            description = spec.description
            if name not in _AGENT_FACTORIES:
                continue
            module_path = _AGENT_FACTORIES[name]  # 模块路径
            module = import_module(module_path)
            factory = getattr(module, "factory")
            runnable = await factory()
            subagents.append(
                {"name": name, "description": description, "runnable": runnable}
            )

        return subagents

    async def factory(self) -> Runnable:
        agent_config = await get_agent_spec(self.name)
        self.agent_config = agent_config
        model = self._get_model()
        tools = await self._get_tools()
        subagents = await self._get_subagents()
        agent = await self.create_agent(
            model=model,
            system_prompt=agent_config.system_prompt,
            tools=tools,
            subagents=subagents,
        )
        return agent

    def _skill_sources(self) -> list[str] | None:
        if not self.agent_config or not self.agent_config.skills:
            return None
        return [f"{agent_skills_root(self.name)}/"]

    def _create_deep_agent_backend(self) -> BackendProtocol:
        return SandboxBackend()

    def _create_deep_agent_middleware(self) -> Sequence[AgentMiddleware]:
        return ()

    def _create_deep_agent_state_schema(self) -> type | None:
        return None

    async def create_agent(
        self,
        *,
        model: Any,
        system_prompt: str,
        tools: Sequence[BaseTool],
        subagents: list[dict],
    ) -> Runnable:
        return create_deep_agent(
            model=model,
            system_prompt=system_prompt,
            tools=tools,
            subagents=[CompiledSubAgent(**config) for config in subagents],
            skills=self._skill_sources(),
            state_schema=self._create_deep_agent_state_schema(),
            middleware=list(self._create_deep_agent_middleware()),
            backend=self._create_deep_agent_backend(),
        )
