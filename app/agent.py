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
    clarification: str  # note about fuzzy matches or substitutions
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

The question may be in Georgian or English.

Reply DATABASE if any of these are true:
- The question asks for data, calculations, aggregations, or breakdowns not already in the history
- The question involves a time period, grouping, or filter not previously computed
- The exact numbers needed are not already stated in the history

Reply HISTORY only if the answer can be derived purely by arithmetic or rephrasing from numbers already stated in the history.

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

Important: If the question asks for "main", "biggest", "top", "most", "largest", or their Georgian equivalents (მთავარი, ყველაზე დიდი, etc.), include ALL breakdown/grouping columns (subcategory, department, description, etc.) so the answer can identify which specific item ranks highest.

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
    """Agent: Generate SQL and note any fuzzy value matches or substitutions."""
    history_ctx = _format_history(state["history"])

    prompt = f"""{history_ctx}Current question: {state['question']}

You are a SQL expert. Analyze the question against the schema and produce a JSON response.

Schema: {json.dumps(state['schema'], ensure_ascii=False)}

STRICT RULES:
- Use ONLY column names that exist in the schema. NEVER invent column names.
- Use ONLY values from the sample values lists. Copy them character-for-character — including Georgian script. NEVER translate, transliterate, or rephrase values.
- If the user typed a specific word intending it as a WHERE filter value (e.g. "show me xpnses" → they meant the value 'expense'; "income" → they meant the value 'revenue') but it doesn't exactly match any schema sample value, use the closest schema value in the WHERE clause and set clarification to explain the substitution (e.g. "User said 'xpnses', used 'expense' instead").
- Leave clarification EMPTY ("") for ALL other cases: conceptual questions ("main expense", "biggest cost", "top revenue", "მთავარი ხარჯი"), general aggregations, time-based questions, comparisons. These are semantic questions, not value lookups.
- For general questions (e.g. "how much revenue"), include ALL matching subcategories using GROUP BY — never filter to just one.
- For comparisons, use GROUP BY or CASE WHEN — never UNION.
- For time-based grouping ("by week", "by month", "by year", "კვირის ჭრილში", "თვის ჭრილში"), use GROUP BY DATE_TRUNC(...) to show ALL periods — never filter to a single hardcoded date. If the date column type is 'text', cast it: DATE_TRUNC('week', date_column::date). Example: SELECT DATE_TRUNC('week', date::date) AS week, SUM(amount) FROM t GROUP BY week ORDER BY week.
- Use conversation history only to resolve references like "them", "it", "same".

Reply with this JSON:
{{
  "sql": "<the SELECT query>",
  "clarification": "<empty string if no substitutions; otherwise explain what term was substituted and why>"
}}

JSON:"""

    response = llm.invoke(prompt)

    try:
        parsed = json.loads(_extract_json(response.content))
        state["sql"] = _clean_sql(parsed.get("sql", ""))
        state["clarification"] = parsed.get("clarification", "")
    except Exception:
        # Fallback: treat entire response as SQL
        state["sql"] = _clean_sql(response.content)
        state["clarification"] = ""

    return state


def validate_sql(state: AgentState) -> AgentState:
    """Structural check only — always execute; errors are caught in execute_sql_node."""
    sql = state["sql"].strip().upper()
    state["valid"] = sql.startswith("SELECT") and "FROM" in sql
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

Answer this question using only the conversation history above. Be concise and direct.
- If the question asks to exclude or deduct something (e.g. "without X"), calculate it from the numbers in history.
- Respond in the same language as the question (Georgian or English)."""
        response = llm.invoke(prompt)
        state["answer"] = response.content.strip()
        return state

    if not state["valid"] or not state["rows"]:
        state["answer"] = state.get("error") or "Could not answer the question with the available data."
        return state

    clarification_ctx = f"Note about query: {state['clarification']}\n" if state["clarification"] else ""

    rows = state["rows"][:10]
    # Pre-compute numeric totals to avoid LLM arithmetic errors
    numeric_totals = {}
    for row in rows:
        for k, v in row.items():
            try:
                numeric_totals[k] = numeric_totals.get(k, 0) + float(v)
            except (TypeError, ValueError):
                pass
    totals_ctx = f"Pre-computed column totals (use these exactly): {json.dumps(numeric_totals, default=str)}\n" if numeric_totals else ""

    prompt = f"""{history_ctx}{clarification_ctx}{totals_ctx}Current question: {state['question']}
Results: {json.dumps(rows, default=str, ensure_ascii=False)}

Answer the current question clearly and concisely based on the results. Follow these rules:
- If a substitution was made (see note above), start with: "I couldn't find '[original term]', but found '[substituted term]', so:"
- If results contain multiple subcategories that together answer the question, list each one with its value, then provide the total. Use the pre-computed totals above — do not recalculate.
- Do not translate Georgian values — use them as-is.
- Respond in the same language as the question (Georgian or English)."""

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
        "clarification": "",
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
