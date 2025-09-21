from langgraph.graph import StateGraph, END
from typing import TypedDict, Annotated
class AppState(TypedDict):
    message: Annotated[str, "The message to print"]

# Define the function node
def greet(state: AppState) -> AppState:
    print("✅ LangGraph is working! Message:", state["message"])
    return state  
graph = StateGraph(AppState)
graph.add_node("greet_node", greet)
graph.set_entry_point("greet_node")

# Compile and invoke the graph
compiled = graph.compile()
compiled.invoke({"message": "Hello from your LangGraph test!"})
