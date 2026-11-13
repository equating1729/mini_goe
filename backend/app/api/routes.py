import os
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.core.config import settings
from app.services.db.database import (
    get_latest_articles,
    get_article_stats,
    search_articles,
)
from app.services.live_data import handle_live_query
from app.services.ner_services import get_top_entities, get_entity_connections

router = APIRouter()


class QueryRequest(BaseModel):
    question: str
    domain: str = "ALL"


PERSONAS = {
    "ALL": "a multi-domain strategic intelligence analyst",
    "GEO": "a geopolitical and foreign-policy analyst",
    "DEFENSE": "a defense and military intelligence analyst",
    "TECH": "a technology and cyber intelligence analyst",
    "CLIMATE": "a climate and energy security analyst",
    "ECONOMICS": "an economic intelligence analyst",
    "SOCIETY": "a social intelligence analyst",
}

_gemini_client = None


def _gemini():
    """Create the client lazily so a missing key cannot crash the whole API."""
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        key = settings.GEMINI_API_KEY or os.getenv("GEMINI_API_KEY", "")
        if not key:
            raise RuntimeError("GEMINI_API_KEY is not set")
        _gemini_client = genai.Client(api_key=key)
    return _gemini_client


def _sources(articles):
    return [
        {"title": a.get("title"), "source": a.get("source"), "url": a.get("url")}
        for a in articles
    ]


@router.get("/health")
def health():
    return {"status": "GOE backend running", "version": "2.1.0"}


@router.get("/domains")
def get_domains():
    return {
        "domains": [
            {"id": "ALL", "name": "All Domains"},
            {"id": "GEO", "name": "Geopolitical"},
            {"id": "DEFENSE", "name": "Defense & Military"},
            {"id": "TECH", "name": "Technology & Cyber"},
            {"id": "CLIMATE", "name": "Climate & Energy"},
            {"id": "ECONOMICS", "name": "Economics"},
            {"id": "SOCIETY", "name": "Society"},
        ]
    }


@router.post("/query")
def query(request: QueryRequest):
    question = request.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    try:
        # 1) Clearly live-data questions (gold, bitcoin, FX, Delhi weather)
        live = handle_live_query(question)
        if live:
            return {"question": question, "answer": live, "sources": []}

        # 2) Retrieve articles from Postgres
        articles = search_articles(question, request.domain, limit=6)
        context = ""
        for i, a in enumerate(articles, 1):
            context += (
                f"\n[Article {i} | {a.get('source', '')} | {a.get('published_at', '')}]\n"
                f"{a.get('title', '')}. {(a.get('body') or '')[:500]}\n"
            )

        persona = PERSONAS.get(request.domain, PERSONAS["ALL"])
        prompt = f"""You are {persona} focused on India's strategic interests.
Answer using the articles below and relate the answer to India's perspective.
Keep it to 3-5 sentences. If the articles do not cover the question, say so
briefly, then give a short answer from general knowledge and mark it as such.

Articles:
{context}

Question: {question}

Answer:"""

        # 3) Gemini; if it fails, fall back to the retrieved headlines
        try:
            resp = _gemini().models.generate_content(
                model=settings.GEMINI_MODEL, contents=prompt
            )
            answer = (resp.text or "").strip()
            if not answer:
                raise RuntimeError("Gemini returned an empty response")
            return {"question": question, "answer": answer, "sources": _sources(articles)}
        except Exception:
            traceback.print_exc()
            headlines = "\n".join(
                f"- {a.get('title')} ({a.get('source')})" for a in articles[:5]
            )
            return {
                "question": question,
                "answer": "The AI model is unavailable right now. "
                          "Most relevant articles:\n" + headlines,
                "sources": _sources(articles),
            }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/articles")
def articles(
    limit: Optional[int] = Query(default=None, description="Max articles; omit for all"),
    domain: str = Query(default="ALL", description="GEO, DEFENSE, TECH, CLIMATE, ECONOMICS, SOCIETY or ALL"),
):
    return get_latest_articles(limit, domain)


@router.get("/stats")
def stats(domain: str = "ALL"):
    return get_article_stats(domain)


@router.get("/graph/entities")
def graph_entities(
    limit: int = Query(default=100, description="Number of entities to return"),
    domain: str = Query(default="ALL", description="Filter by domain"),
):
    return get_top_entities(limit, domain)


@router.get("/graph/entity/{name}")
def entity_connections(
    name: str,
    domain: str = Query(default="ALL", description="Filter by domain"),
):
    return get_entity_connections(name, domain)