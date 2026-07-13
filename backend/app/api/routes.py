from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import sys
import os
from typing import Optional

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../ml")
))

from app.services.db.database import get_latest_articles, get_article_stats
from app.services.ner_services import get_top_entities, get_entity_connections
from rag.rag import query_rag, index_articles

# ✅ Make sure router is defined here
router = APIRouter()

class QueryRequest(BaseModel):
    question: str
    domain: str = "ALL"

@router.get("/health")
def health():
    return {"status": "GOE backend running", "version": "2.0.0"}

@router.get("/domains")
def get_domains():
    """Get available domains and their descriptions"""
    return {
        "domains": [
            {
                "id": "ALL",
                "name": "All Domains",
                "description": "Aggregated intelligence across all domains",
                "icon": "🌐"
            },
            {
                "id": "GEO",
                "name": "Geopolitical",
                "description": "International relations and foreign policy analysis",
                "icon": "🌍"
            },
            {
                "id": "DEFENSE",
                "name": "Defense & Military",
                "description": "Military strategy, security, and defense intelligence",
                "icon": "⚔️"
            },
            {
                "id": "TECH",
                "name": "Technology & Cyber",
                "description": "Technology, cyber security, and space intelligence",
                "icon": "💻"
            },
            {
                "id": "CLIMATE",
                "name": "Climate & Energy",
                "description": "Climate security, energy, and environmental intelligence",
                "icon": "🌿"
            }
        ]
    }

@router.post("/query")
def query(request: QueryRequest):
    if not request.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")
    try:
        from app.services.db.database import get_latest_articles
        from groq import Groq
        import os

        groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

        # Get domain from request
        domain = getattr(request, 'domain', 'ALL')

        # Fetch relevant articles from Supabase
        articles = get_latest_articles(limit=10, domain=domain if domain != 'ALL' else None)

        if not articles:
            articles = get_latest_articles(limit=10)

        # Build context
        context = ""
        for i, article in enumerate(articles[:5]):
            context += f"\n[Article {i+1} — {article.get('source', '')} — {article.get('published_at', '')}]\n{article.get('title', '')}. {article.get('body', '') or ''}\n"

        DOMAIN_PERSONALITIES = {
            "DEFENSE": "You are a strategic defense and military intelligence analyst focused on India's security interests.",
            "TECH": "You are a technology and cyber intelligence analyst focused on India's technological interests.",
            "CLIMATE": "You are a climate security analyst focused on India's environmental interests.",
            "GEO": "You are a geopolitical analyst focused on India's international relations.",
            "ALL": "You are a strategic multi-domain intelligence analyst focused on India's national interests.",
            "ECONOMICS": "You are an economic intelligence analyst focused on India's economic interests.",
            "SOCIETY": "You are a social intelligence analyst focused on India's societal developments.",
        }

        personality = DOMAIN_PERSONALITIES.get(domain, DOMAIN_PERSONALITIES["ALL"])

        prompt = f"""{personality}

Answer the question below using the provided articles from GOE intelligence database.
Always relate your answer to India's perspective and interests.
Keep the answer concise — 3 to 5 sentences.
If articles are not relevant, use your general knowledge but mention it.

Articles:
{context}

Question: {request.question}

Answer:"""

        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500
        )

        answer = response.choices[0].message.content
        return {"question": request.question, "answer": answer}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/articles")
def articles(
    limit: Optional[int] = Query(default=None, description="Number of articles to return. If not provided, returns ALL articles."),
    domain: str = Query(default="ALL", description="Filter by domain (GEO, DEFENSE, TECH, CLIMATE, ALL)")
):
    """
    Get articles with optional limit.
    - If limit is not provided: returns ALL articles
    - If limit is provided: returns only that many articles
    - domain filters by specific domain
    """
    return get_latest_articles(limit, domain)

@router.get("/stats")
def stats(domain: str = "ALL"):
    return get_article_stats(domain)

@router.get("/graph/entities")
def graph_entities(
    limit: int = Query(default=100, description="Number of entities to return"),
    domain: str = Query(default="ALL", description="Filter by domain")
):
    return get_top_entities(limit, domain)

@router.get("/graph/entity/{name}")
def entity_connections(
    name: str,
    domain: str = Query(default="ALL", description="Filter by domain")
):
    return get_entity_connections(name, domain)

@router.post("/index")
def index():
    try:
        result = index_articles()
        return {"status": "indexing complete", **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))