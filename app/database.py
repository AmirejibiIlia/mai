from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from contextlib import contextmanager
from typing import Dict, List
import json
from .config import get_settings
from .exceptions import CompanyNotFoundError

settings = get_settings()

engine = create_engine(
    settings.DATABASE_URL,
    poolclass=NullPool,
    connect_args={"connect_timeout": 10}
)

@contextmanager
def get_db():
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()

def load_metadata(company_id: str) -> Dict:
    with get_db() as conn:
        result = conn.execute(
            text("SELECT metadata FROM company_metadata WHERE company_id = :id"),
            {"id": company_id}
        )
        row = result.fetchone()
        if not row:
            raise CompanyNotFoundError(company_id)
        return json.loads(row[0])

def execute_query(sql: str, timeout: int = None) -> List[Dict]:
    with get_db() as conn:
        if timeout:
            conn.execute(text(f"SET statement_timeout = {timeout * 1000}"))
        result = conn.execute(text(sql))
        return [dict(r._mapping) for r in result.fetchmany(settings.MAX_SQL_ROWS)]

def get_companies() -> List[str]:
    with get_db() as conn:
        result = conn.execute(text("SELECT company_id FROM company_metadata ORDER BY company_id"))
        return [row[0] for row in result]