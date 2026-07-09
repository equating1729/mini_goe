from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sys
import os

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../ml")
))

from app.services.db.database import get_latest_articles, get_article_stats
from app.services.ner_services import get_top_entities, get_entity_connections
from rag.rag import query_rag, index_articles

router = APIRouter()

class QueryRequest(BaseModel):
    question: str

@router.get("/health")
def health():
    return {"status": "GOE backend running"}

@router.post("/query")
def query(request: QueryRequest):
    if not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        answer = query_rag(request.question)
        return {
            "question": request.question,
            "answer": answer
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/articles")
def articles(limit: int = 20, domain: str = None):
    return get_latest_articles(limit, domain)

@router.get("/stats")
def stats():
    return get_article_stats()

@router.get("/graph/entities")
def graph_entities(limit: int = 50, domain: str = None):
    return get_top_entities(limit, domain)

@router.get("/graph/entity/{name}")
def entity_connections(name: str):
    return get_entity_connections(name)

@router.post("/index")
def index():
    try:
        index_articles()
        return {"status": "indexing complete"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))