from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from pydantic import BaseModel
from .agent import run
from .database import get_companies, engine, create_logs_table, insert_log, update_log_rating
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up...")
    create_logs_table()
    yield
    logger.info("Shutting down...")
    engine.dispose()

app = FastAPI(title="AI SQL Agent", version="1.0.0", lifespan=lifespan)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    company_id: str
    question: str
    history: list[dict] = []

class LogRequest(BaseModel):
    company_id: str
    question: str
    answer: str

class RatingRequest(BaseModel):
    rating: int

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

@app.post("/log")
def log(req: LogRequest):
    try:
        log_id = insert_log(req.company_id, req.question, req.answer)
        return {"log_id": log_id}
    except Exception as e:
        logger.error(f"Log failed: {e}", exc_info=True)
        raise HTTPException(500, "Failed to save log")

@app.patch("/log/{log_id}")
def rate(log_id: int, req: RatingRequest):
    if not 1 <= req.rating <= 10:
        raise HTTPException(400, "Rating must be between 1 and 10")
    try:
        update_log_rating(log_id, req.rating)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Rating failed: {e}", exc_info=True)
        raise HTTPException(500, "Failed to save rating")
