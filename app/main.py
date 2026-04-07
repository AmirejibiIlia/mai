from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from .agent import run
from .database import get_companies, engine
from .config import get_settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    yield
    logger.info("Shutting down...")
    engine.dispose()

app = FastAPI(title="AI SQL Agent", version="1.0.0", lifespan=lifespan)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    company_id: str
    question: str
    history: list[dict] = []

@app.get("/")
def health():
    return {"status": "healthy", "version": "1.0.0"}

@app.get("/companies")
def list_companies():
    return {"companies": get_companies()}

@app.post("/query")
def query(req: QueryRequest):
    try:
        result = run(req.company_id, req.question, req.history)
        return {
            "company_id": req.company_id,
            "answer": result["answer"],
            "sql": result["sql"],
            "valid": result["valid"],
            "rows": result["rows"]
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query failed: {e}", exc_info=True)
        raise HTTPException(500, "Internal server error")
