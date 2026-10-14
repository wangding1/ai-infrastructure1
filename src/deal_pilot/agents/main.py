from collections.abc import Sequence

from langchain.agents.middleware.types import AgentMiddleware

from deal_pilot.agents.agent_factory import AgentFactory
from deal_pilot.agents.main_agent_state import MainAgentState
from deal_pilot.middlewares.turn_summary import turn_summary
from deal_pilot.utils.trace_wrapper import trace_wrapper


class MainAgentFactory(AgentFactory):
    @property
    def name(self) -> str:
        return "main_agent"

    def _create_deep_agent_middleware(self) -> Sequence[AgentMiddleware]:
        return (turn_summary,)

    def _create_deep_agent_state_schema(self) -> type | None:
        return MainAgentState


main_agent_factory = MainAgentFactory()

factory = main_agent_factory.factory

factory_with_trace = trace_wrapper(factory)
