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
    plan: str           # "database" or "history"
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
    if not history:
        return ""
    lines = ["Conversation history:"]
    for msg in history[-6:]:
        role = "User" if msg.get("role") == "user" else "Assistant"
        lines.append(f"  {role}: {msg.get('content', '')}")
    return "\n".join(lines) + "\n\n"


def planner(state: AgentState) -> AgentState:
    """Decide whether to answer from conversation history or query the database."""
    if not state["history"]:
        state["plan"] = "database"
        return state

    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}New question: {state['question']}

The question may be in Georgian or English. Can it be fully and accurately answered using only the conversation history above, without querying the database?

Reply with only DATABASE or HISTORY."""

    response = llm.invoke(prompt)
    state["plan"] = "history" if "HISTORY" in response.content.upper() else "database"
    return state


def select_schema(state: AgentState) -> AgentState:
    """Agent: Pick only the tables and columns needed to answer the question."""
    tables = state["metadata"].get("tables", {})
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}

You are a database expert. The question and data may be in Georgian. Identify the minimum tables and columns needed to answer the current question (use conversation history only to understand references like "that", "it", "the same" or their Georgian equivalents).

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
    """Agent: Generate an efficient SQL query from the focused schema."""
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}

You are a SQL expert. Write a single efficient SELECT query to answer the current question.
Schema: {json.dumps(state['schema'], ensure_ascii=False)}

STRICT RULES — violating these will cause the query to fail:
- Use ONLY column names that exist in the schema above. NEVER invent column names.
- Use ONLY values that appear in the sample values lists above. Copy them character-for-character — including Georgian script. NEVER translate, transliterate, or rephrase values.
- Use conversation history only to resolve what "them", "it", "same" or their Georgian equivalents refer to.
- For comparisons or multi-part questions, return all relevant grouped data — the answer agent will synthesize the final response from the rows.
- For comparisons, use GROUP BY or CASE WHEN aggregations — never UNION.
- Return ONLY the SQL query, no explanation.

SQL:"""

    response = llm.invoke(prompt)
    state["sql"] = _clean_sql(response.content)
    return state


def validate_sql(state: AgentState) -> AgentState:
    """Agent: Verify the SQL actually answers what the user asked."""
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}
SQL: {state['sql']}

The question and data may be in Georgian. Does this SQL return data that is sufficient to answer the current question (even if the final answer requires summing or interpreting the rows)? Reply with only YES or NO."""

    response = llm.invoke(prompt)
    state["valid"] = "YES" in response.content.upper()
    return state


def execute_sql_node(state: AgentState) -> AgentState:
    if not state["valid"]:
        state["rows"] = []
        return state

    company_filter = state["metadata"]["company_filter"]

    def inject_filter(query: str) -> str:
        q_upper = query.upper()
        if "WHERE" in q_upper:
            idx = q_upper.index("WHERE")
            return query[:idx + 5] + f" {company_filter} AND" + query[idx + 5:]
        insert_pos = len(query)
        for kw in ["GROUP BY", "ORDER BY", "HAVING", "LIMIT"]:
            idx = q_upper.find(kw)
            if idx != -1 and idx < insert_pos:
                insert_pos = idx
        return query[:insert_pos].rstrip() + f" WHERE {company_filter} " + query[insert_pos:]

    # Handle UNION — inject filter into each SELECT part
    parts = state["sql"].rstrip(";").split("UNION")
    sql = " UNION ".join(inject_filter(p.strip()) for p in parts)

    try:
        state["rows"] = execute_query(sql, settings.QUERY_TIMEOUT)
    except Exception as e:
        state["error"] = str(e)
        state["valid"] = False
        state["rows"] = []

    return state


def format_answer(state: AgentState) -> AgentState:
    history_ctx = _format_history(state["history"])

    if state["plan"] == "history":
        prompt = f"""{history_ctx}Question: {state['question']}

Answer this question using only the conversation history above. Be concise and direct. Respond in the same language as the question (Georgian or English)."""
        response = llm.invoke(prompt)
        state["answer"] = response.content.strip()
        return state

    if not state["valid"] or not state["rows"]:
        state["answer"] = state.get("error") or "Could not answer the question with the available data."
        return state

    prompt = f"""{history_ctx}Current question: {state['question']}
Results: {json.dumps(state['rows'][:10], default=str, ensure_ascii=False)}

Answer the current question clearly and concisely based on the results.
Give a short, direct answer in the same language as the question (Georgian or English). Do not translate Georgian values in the results — use them as-is when referencing them."""

    response = llm.invoke(prompt)
    state["answer"] = response.content.strip()
    return state


def route_plan(state: AgentState) -> Literal["select_schema", "format"]:
    return "select_schema" if state["plan"] == "database" else "format"


def route_validate(state: AgentState) -> Literal["execute", "format"]:
    return "execute" if state["valid"] else "format"


_graph = None


def get_graph():
    global _graph
    if _graph is None:
        workflow = StateGraph(AgentState)
        workflow.add_node("planner", planner)
        workflow.add_node("select_schema", select_schema)
        workflow.add_node("generate_sql", generate_sql)
        workflow.add_node("validate", validate_sql)
        workflow.add_node("execute", execute_sql_node)
        workflow.add_node("format", format_answer)
        workflow.set_entry_point("planner")
        workflow.add_conditional_edges("planner", route_plan, {"select_schema": "select_schema", "format": "format"})
        workflow.add_edge("select_schema", "generate_sql")
        workflow.add_edge("generate_sql", "validate")
        workflow.add_conditional_edges("validate", route_validate, {"execute": "execute", "format": "format"})
        workflow.add_edge("execute", "format")
        workflow.add_edge("format", END)
        _graph = workflow.compile()
    return _graph


def run(company_id: str, question: str, history: list = []) -> dict:
    result = get_graph().invoke({
        "company_id": company_id,
        "question": question,
        "history": history,
        "plan": "database",
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
