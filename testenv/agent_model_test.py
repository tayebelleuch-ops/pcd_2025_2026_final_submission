from google.adk.agents.llm_agent import Agent

root_agent = Agent(
    model='gemini-3.1-flash',
    name='root_agent',
    description='A helpful assistant for user questions. Routes queries to the appropriate database expert.',
    instruction='hi')