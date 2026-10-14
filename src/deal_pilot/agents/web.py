from deal_pilot.agents.agent_factory import AgentFactory
from deal_pilot.utils.trace_wrapper import trace_wrapper


class WebAgentFactory(AgentFactory):
    @property
    def name(self) -> str:
        return "web_agent"


web_agent_factory = WebAgentFactory()

factory = web_agent_factory.factory

factory_with_trace = trace_wrapper(factory)
