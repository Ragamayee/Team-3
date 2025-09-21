from typing import TypedDict, Annotated, Literal
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain.agents import Tool
import os

os.environ["OPENAI_API_KEY"] = "your-key-here"
from langchain_openai import ChatOpenAI

llm = ChatOpenAI("OPEN-API-KEY")



import re

@tool("simple_math", return_direct=True)
def simple_math(expression: str) -> str:
    """Solve a math expression like 4 + 5 * 2."""
    try:
       
        clean_expr = "".join(re.findall(r"[0-9\.\+\-\*\/\(\)\s]", expression))
        result = eval(clean_expr, {"__builtins__": None}, {})
        return str(result)
    except Exception as e:
        return f"Error in expression: {str(e)}"



class AgentState(TypedDict):
    input: Annotated[str, "User input"]
    output: Annotated[str, "Agent response"]

def router(state: AgentState) -> str:
    if any(op in state["input"] for op in "+-*/="):
        return "math"
    return "chat"

def math(state: AgentState) -> AgentState:
    result = simple_math.invoke(state["input"])
    return {"input": state["input"], "output": result}

def chat(state: AgentState) -> AgentState:
    result = llm.invoke(state["input"])
    return {"input": state["input"], "output": result.content}


graph = StateGraph(AgentState)
graph.add_node("math", math)
graph.add_node("chat", chat)
graph.add_conditional_edges("router", router, {"math": "math", "chat": "chat"})
graph.add_node("router", lambda x: x)  
graph.set_entry_point("router")
graph.add_edge("math", END)
graph.add_edge("chat", END)

app = graph.compile()

user_input = input("You: ")
result = app.invoke({"input": user_input})
print("Agent:", result["output"])
