import sys
import os

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../ingestion"))
)

from store import get_connection
import chromadb
from chromadb.utils import embedding_functions
import ollama

CHROMA_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../data/chroma_db"
)

chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2"
)

collection = chroma_client.get_or_create_collection(
    name="goe_articles",
    embedding_function=embedding_fn
)

# ==========================================================
# DOMAIN PERSONALITIES
# ==========================================================

DOMAIN_PERSONALITIES = {
    "DEFENSE": """
You are a strategic defense and military intelligence analyst focused on India's security interests.
Analyze military developments, defense strategies, border security, naval activities,
air power, intelligence operations, and regional military balance.
Give concise, factual, and strategic answers.
""",

    "TECH": """
You are a technology, cyber, and space intelligence analyst focused on India's technological interests.
Analyze AI, cyber security, semiconductors, telecom, quantum computing,
space programs, digital infrastructure, and emerging technologies.
Give concise, factual, and strategic answers.
""",

    "CLIMATE": """
You are a climate security and energy intelligence analyst focused on India's environmental interests.
Analyze climate change, renewable energy, disasters, water security,
food security, environmental policy, and sustainability.
Give concise, factual, and strategic answers.
""",

    "GEO": """
You are a geopolitical and foreign policy intelligence analyst focused on India's international interests.
Analyze diplomacy, global conflicts, trade, alliances, strategic partnerships,
international organizations, and geopolitical developments.
Give concise, factual, and strategic answers.
""",

    "ALL": """
You are a strategic multi-domain intelligence analyst focused on India's national interests.
Combine geopolitical, defense, technology, cyber, economic,
and climate perspectives when answering.
Give concise, factual, and strategic answers.
"""
}


# ==========================================================
# INDEX ARTICLES
# ==========================================================

def index_articles():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, title, body, source, published_at FROM articles"
    )

    articles = cursor.fetchall()

    conn.close()

    print(f"Indexing {len(articles)} articles into ChromaDB...")

    existing = set(collection.get()["ids"])

    docs = []
    ids = []
    metas = []

    for article in articles:
        doc_id = f"article_{article['id']}"

        if doc_id in existing:
            continue

        text = f"{article['title']}. {article['body']}"

        docs.append(text)
        ids.append(doc_id)

        metas.append({
            "source": article["source"],
            "published_at": article["published_at"]
        })

    if docs:
        collection.add(
            documents=docs,
            ids=ids,
            metadatas=metas
        )

        print(f"Added {len(docs)} new articles to ChromaDB.")

    else:
        print("No new articles to index.")


# ==========================================================
# QUERY RAG
# ==========================================================

def query_rag(
    question: str,
    domain: str = "ALL",
    n_results: int = 3
) -> str:

    results = collection.query(
        query_texts=[question],
        n_results=n_results
    )

    chunks = results["documents"][0] if results["documents"] else []
    sources = results["metadatas"][0] if results["metadatas"] else []

    if not chunks:
        return "No relevant articles found in the database."

    context = ""

    for i, (chunk, meta) in enumerate(zip(chunks, sources)):
        context += (
            f"\n[Article {i+1} — "
            f"{meta.get('source', 'unknown')} — "
            f"{meta.get('published_at', '')}]\n"
            f"{chunk}\n"
        )

    personality = DOMAIN_PERSONALITIES.get(
        domain.upper(),
        DOMAIN_PERSONALITIES["ALL"]
    )

    prompt = f"""
{personality}

Answer the question using ONLY the provided articles.

Always relate your answer to India's perspective and interests.

If the articles do not contain enough information,
clearly state that.

Keep the answer concise (3–5 sentences).

Articles:
{context}

Question:
{question}

Answer:
"""

    try:
        response = ollama.chat(
            model="llama3.2",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )

        return response["message"]["content"]

    except Exception as e:
        return f"Error querying Ollama: {str(e)}"


# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    index_articles()

    print("\n--- Testing RAG ---")

    tests = [
        (
            "What are the latest developments in India's foreign relations?",
            "GEO"
        ),
        (
            "What are India's latest defense developments?",
            "DEFENSE"
        ),
        (
            "What are the latest AI developments?",
            "TECH"
        ),
        (
            "How is climate change affecting India?",
            "CLIMATE"
        ),
        (
            "Give an overview of India's strategic situation.",
            "ALL"
        )
    ]

    for question, domain in tests:

        print("\n========================================")
        print(f"Domain : {domain}")
        print(f"Question: {question}")
        print("----------------------------------------")

        answer = query_rag(
            question=question,
            domain=domain
        )

        print(answer)