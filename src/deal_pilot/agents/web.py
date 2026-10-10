from langchain.agents import create_agent
from deal_pilot.tools import web_search, fetch_url, get_current_time
from deal_pilot.utils.create_model import create_model
from deal_pilot.utils.trace_wrapper import trace_wrapper
from deal_pilot.utils.get_prompt import get_prompt
from deal_pilot.middlewares.link_prompt_middleware import create_prompt_link_middleware


async def agent_factory():
    prompt = await get_prompt("web_agent")
    model = create_model(**prompt.config.get("model_config", {}))
    agent = create_agent(
        model=model,
        tools=[web_search, fetch_url, get_current_time],
        system_prompt=prompt.compile(),
        middleware=[create_prompt_link_middleware(prompt)],
    )
    return agent


agent_factory_with_trace = trace_wrapper(agent_factory)
