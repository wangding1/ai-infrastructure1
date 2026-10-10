from deal_pilot.tools import get_current_time, ask_user_question
from deal_pilot.agents.web import agent_factory as web_agent_factory
from deal_pilot.utils.create_model import create_model
from deepagents import create_deep_agent
from deepagents.middleware import CompiledSubAgent
from deal_pilot.utils.trace_wrapper import trace_wrapper
from deal_pilot.utils.get_prompt import get_prompt
from deal_pilot.middlewares.link_prompt_middleware import create_prompt_link_middleware
from deal_pilot.agents.main_agent_state import MainAgentState
from deal_pilot.middlewares.turn_summary import turn_summary


async def agent_factory():
    web_agent = await web_agent_factory()
    prompt = await get_prompt("main_agent")
    model = create_model(**prompt.config.get("model_config", {}))
    web_agent_prompt = await get_prompt("web_agent")
    web_sub_agent = CompiledSubAgent(
        name="web_agent",
        description=web_agent_prompt.config.get("description", ""),
        runnable=web_agent,
    )
    agent = create_deep_agent(
        model=model,
        system_prompt=prompt.compile(),
        tools=[get_current_time, ask_user_question],
        state_schema=MainAgentState,
        subagents=[web_sub_agent],
        middleware=[create_prompt_link_middleware(prompt), turn_summary],
    )
    return agent


agent_factory_with_trace = trace_wrapper(agent_factory)
