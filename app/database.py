from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from contextlib import contextmanager
from typing import Dict, List
import json
from .config import get_settings
from .exceptions import CompanyNotFoundError

settings = get_settings()

_db_url = settings.DATABASE_URL.replace("postgresql://", "postgresql+pg8000://", 1).replace("postgres://", "postgresql+pg8000://", 1)

engine = create_engine(
    _db_url,
    poolclass=NullPool,
    connect_args={"timeout": 10}
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
        data = row[0]
        return data if isinstance(data, dict) else json.loads(data)

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

def create_logs_table():
    with get_db() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                company_id VARCHAR(255),
                question TEXT,
                answer TEXT,
                rating INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()

def insert_log(company_id: str, question: str, answer: str) -> int:
    with get_db() as conn:
        result = conn.execute(
            text("INSERT INTO logs (company_id, question, answer) VALUES (:c, :q, :a) RETURNING id"),
            {"c": company_id, "q": question, "a": answer}
        )
        log_id = result.fetchone()[0]
        conn.commit()
        return log_id

def update_log_rating(log_id: int, rating: int):
    with get_db() as conn:
        conn.execute(
            text("UPDATE logs SET rating = :r WHERE id = :id"),
            {"r": rating, "id": log_id}
        )
        conn.commit()