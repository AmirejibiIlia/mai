from langgraph.graph import StateGraph, END
from langchain_groq import ChatGroq
from typing import TypedDict, Literal
from .config import get_settings
from .database import load_metadata, execute_query
import json

settings = get_settings()
llm = ChatGroq(api_key=settings.GROQ_API_KEY, model=settings.LLM_MODEL, temperature=settings.LLM_TEMPERATURE)


class AgentState(TypedDict):
    company_id: str
    question: str
    history: list
    metadata: dict
    schema: dict
    sql: str
    valid: bool
    rows: list
    answer: str
    error: str | None


def _clean_sql(sql: str) -> str:
    if "```" in sql:
        parts = sql.split("```")
        sql = parts[1] if len(parts) > 1 else parts[0]
        if sql.lower().startswith("sql"):
            sql = sql[3:]
    return sql.strip()


def _extract_json(text: str) -> str:
    if "```" in text:
        parts = text.split("```")
        text = parts[1] if len(parts) > 1 else parts[0]
        if text.lower().startswith("json"):
            text = text[4:]
    return text.strip()


def _format_history(history: list) -> str:
    """Format last 6 messages as readable context block."""
    if not history:
        return ""
    lines = ["Conversation history:"]
    for msg in history[-6:]:
        role = "User" if msg.get("role") == "user" else "Assistant"
        lines.append(f"  {role}: {msg.get('content', '')}")
    return "\n".join(lines) + "\n\n"


def select_schema(state: AgentState) -> AgentState:
    """Agent 1: Pick only the tables and columns needed to answer the question."""
    tables = state["metadata"].get("tables", {})
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}

You are a database expert. Identify the minimum tables and columns needed to answer the current question (use conversation history only to understand references like "that", "it", "the same").

Available schema:
{json.dumps(tables, ensure_ascii=False)}

Reply with a JSON object like: {{"table_name": ["col1", "col2"], ...}}

JSON:"""

    response = llm.invoke(prompt)

    try:
        selected = json.loads(_extract_json(response.content))
    except Exception:
        selected = {t: list(cols["columns"].keys()) for t, cols in tables.items()}

    focused = {}
    for table, columns in selected.items():
        if table not in tables:
            continue
        focused[table] = {
            "columns": {c: tables[table]["columns"][c] for c in columns if c in tables[table]["columns"]},
            "values": {c: tables[table]["values"][c] for c in columns if c in tables[table].get("values", {})}
        }

    state["schema"] = focused
    return state


def generate_sql(state: AgentState) -> AgentState:
    """Agent 2: Generate an efficient SQL query from the focused schema."""
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}

You are a SQL expert. Write a single efficient SELECT query to answer the current question.
Schema: {json.dumps(state['schema'], ensure_ascii=False)}

Rules:
- Use exact table/column names from the schema
- Match exact values from sample values where applicable
- Use conversation history only to resolve references (e.g. "same period", "that category")
- Return ONLY the SQL query, no explanation

SQL:"""

    response = llm.invoke(prompt)
    state["sql"] = _clean_sql(response.content)
    return state


def validate_sql(state: AgentState) -> AgentState:
    """Agent 3: Verify the SQL actually answers what the user asked."""
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}
SQL: {state['sql']}

Does this SQL correctly answer the current question? Reply with only YES or NO."""

    response = llm.invoke(prompt)
    state["valid"] = "YES" in response.content.upper()
    return state


def execute_sql_node(state: AgentState) -> AgentState:
    if not state["valid"]:
        state["rows"] = []
        return state

    sql = state["sql"].rstrip(";")
    company_filter = state["metadata"]["company_filter"]
    sql_upper = sql.upper()

    if "WHERE" in sql_upper:
        where_idx = sql_upper.index("WHERE")
        sql = sql[:where_idx + 5] + f" {company_filter} AND" + sql[where_idx + 5:]
    else:
        insert_pos = len(sql)
        for kw in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
            idx = sql_upper.find(kw)
            if idx != -1 and idx < insert_pos:
                insert_pos = idx
        sql = sql[:insert_pos].rstrip() + f" WHERE {company_filter} " + sql[insert_pos:]

    try:
        state["rows"] = execute_query(sql, settings.QUERY_TIMEOUT)
    except Exception as e:
        state["error"] = str(e)
        state["valid"] = False
        state["rows"] = []

    return state


def format_answer(state: AgentState) -> AgentState:
    if not state["valid"] or not state["rows"]:
        state["answer"] = state.get("error") or "Could not answer the question with the available data."
        return state

    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}
Results: {json.dumps(state['rows'][:10], default=str, ensure_ascii=False)}

Answer the current question clearly and concisely based on the results.
Give a short, direct answer in the same language as the question."""

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
        workflow.add_node("select_schema", select_schema)
        workflow.add_node("generate_sql", generate_sql)
        workflow.add_node("validate", validate_sql)
        workflow.add_node("execute", execute_sql_node)
        workflow.add_node("format", format_answer)
        workflow.set_entry_point("select_schema")
        workflow.add_edge("select_schema", "generate_sql")
        workflow.add_edge("generate_sql", "validate")
        workflow.add_conditional_edges("validate", route, {"execute": "execute", "format": "format"})
        workflow.add_edge("execute", "format")
        workflow.add_edge("format", END)
        _graph = workflow.compile()
    return _graph


def run(company_id: str, question: str, history: list = []) -> dict:
    result = get_graph().invoke({
        "company_id": company_id,
        "question": question,
        "history": history,
        "metadata": load_metadata(company_id),
        "schema": {},
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
