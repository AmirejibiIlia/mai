from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from typing import TypedDict, Literal
from .config import get_settings
from .database import load_metadata, execute_query
from .exceptions import InvalidSQLError, QueryExecutionError
import json

settings = get_settings()
llm = ChatGroq(api_key=settings.GROQ_API_KEY, model=settings.LLM_MODEL, temperature=settings.LLM_TEMPERATURE)

class AgentState(TypedDict):
    company_id: str
    question: str
    metadata: dict
    sql: str
    valid: bool
    rows: list
    answer: str
    error: str | None

def _clean_sql(sql: str) -> str:
    if "```" in sql:
        sql = sql.split("```")[1]
        if sql.lower().startswith("sql"):
            sql = sql[3:]
    return sql.strip()

def generate_sql(state: AgentState) -> AgentState:
    prompt = f"""Database schema: {json.dumps(state['metadata'], ensure_ascii=False)}
Question: {state['question']}
Generate ONLY a SELECT query using exact values from schema.
SQL:"""
    
    response = llm.invoke(prompt)
    state["sql"] = _clean_sql(response.content)
    return state

def validate_sql(state: AgentState) -> AgentState:
    prompt = f"""Is this SQL valid for the question?
Question: {state['question']}
SQL: {state['sql']}
Reply: YES or NO
Answer:"""
    
    response = llm.invoke(prompt)
    state["valid"] = "YES" in response.content.upper()
    return state

def execute_sql_node(state: AgentState) -> AgentState:
    if not state["valid"]:
        state["rows"] = []
        return state
    
    sql = state["sql"]
    company_filter = state["metadata"]["company_filter"]
    
    if "WHERE" in sql.upper():
        sql = sql.replace("WHERE", f"WHERE {company_filter} AND", 1)
    else:
        sql = sql.rstrip(";") + f" WHERE {company_filter}"
    
    try:
        state["rows"] = execute_query(sql, settings.QUERY_TIMEOUT)
    except Exception as e:
        state["error"] = str(e)
        state["valid"] = False
        state["rows"] = []
    
    return state

def format_answer(state: AgentState) -> AgentState:
    if not state["valid"]:
        state["answer"] = state.get("error", "Invalid SQL generated")
        return state
    
    prompt = f"""Question: {state['question']}
Results: {json.dumps(state['rows'][:10], default=str, ensure_ascii=False)}
Provide a concise answer in the same language.
Answer:"""
    
    response = llm.invoke(prompt)
    state["answer"] = response.content.strip()
    return state

def route(state: AgentState) -> Literal["execute", "format"]:
    return "execute" if state["valid"] else "format"

_graph = None

def get_graph():
    global _graph
    if _graph is None:
        workflow = StateGraph(AgentState)
        workflow.add_node("generate", generate_sql)
        workflow.add_node("validate", validate_sql)
        workflow.add_node("execute", execute_sql_node)
        workflow.add_node("format", format_answer)
        workflow.set_entry_point("generate")
        workflow.add_edge("generate", "validate")
        workflow.add_conditional_edges("validate", route, {"execute": "execute", "format": "format"})
        workflow.add_edge("execute", "format")
        workflow.add_edge("format", END)
        _graph = workflow.compile()
    return _graph

def run(company_id: str, question: str) -> dict:
    result = get_graph().invoke({
        "company_id": company_id,
        "question": question,
        "metadata": load_metadata(company_id),
        "sql": "",
        "valid": False,
        "rows": [],
        "answer": "",
        "error": None
    })
    
    return {
        "answer": result["answer"],
        "sql": result["sql"],
        "valid": result["valid"],
        "rows": len(result["rows"])
    }