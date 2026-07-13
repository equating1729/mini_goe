import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import json
import re
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[2]  
load_dotenv(BASE_DIR / ".env")

sys.path.insert(0, str(BASE_DIR / "ml" / "ingestion"))
from store import get_connection
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

CHROMA_PATH = os.path.join(os.path.dirname(__file__), "../../data/chroma_db")

chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)

def get_embedding(text: str):
    import hashlib
    words = text.lower().split()[:50]
    vec = [float(int(hashlib.md5(w.encode()).hexdigest()[:8], 16)) / 1e10 for w in words]
    return vec + [0.0] * (50 - len(vec))

class SimpleEmbeddingFunction:
    def __call__(self, input):
        return [get_embedding(text) for text in input]
    def name(self):
        return "simple-hash-embedding"
    def embed_documents(self, input):
        return [get_embedding(text) for text in input]
    
    def embed_query(self, input):
        return [get_embedding(text) for text in input]

collection = chroma_client.get_or_create_collection(
    name="goe_articles_v2",
    embedding_function=SimpleEmbeddingFunction()
)

DOMAIN_PERSONALITIES = {
    "DEFENSE": "You are a strategic defense and military intelligence analyst focused on India's security interests. Analyze military developments, defense strategies, and security threats with precision.",
    "TECH": "You are a technology, cyber, and space intelligence analyst focused on India's technological and strategic cyber interests. Analyze technological developments, cyber threats, and space capabilities.",
    "CLIMATE": "You are a climate security and energy intelligence analyst focused on India's environmental, climate, and energy interests. Analyze climate impacts, energy security, and sustainability challenges.",
    "GEO": "You are a strategic geopolitical and foreign policy intelligence analyst focused on India's international relations. Analyze diplomatic developments, international relations, and global power dynamics.",
    "ALL": "You are a strategic geopolitical and multi-domain intelligence analyst focused on India's national interests. Integrate defense, technology, climate, and diplomatic perspectives in your analysis."
}

# Keywords that indicate the user wants real-time/current data
REAL_TIME_KEYWORDS = [
    "current", "latest", "today", "now", "price", "gold", "silver", "stock", "market",
    "weather", "temperature", "news", "update", "live", "real-time", "current price",
    "exchange rate", "dollar", "rupee", "sensex", "nifty", "covid", "cases", "vaccine",
    "bitcoin", "crypto", "ethereum"
]



def is_real_time_question(question: str) -> bool:
    """Check if the question requires real-time data"""
    question_lower = question.lower()
    
    # Check for price/market questions
    price_patterns = [
        r"price of gold",
        r"gold price",
        r"gold rate",
        r"price of silver",
        r"silver price",
        r"stock price",
        r"share price",
        r"sensex",
        r"nifty",
        r"exchange rate",
        r"usd to inr",
        r"dollar rate",
        r"cryptocurrency",
        r"bitcoin price",
        r"price of bitcoin"
    ]
    
    for pattern in price_patterns:
        if re.search(pattern, question_lower):
            return True
    
    # Check for time-based keywords
    time_keywords = ["today", "current", "latest", "now", "live", "realtime", "real-time", "update"]
    for keyword in time_keywords:
        if keyword in question_lower:
            return True
    
    return False

def is_general_knowledge(question: str) -> bool:
    """Check if the question is general knowledge (can be answered by LLM)"""
    question_lower = question.lower()
    
    # Check for general knowledge patterns
    patterns = [
        r"^what is\s+",
        r"^who is\s+",
        r"^when was\s+",
        r"^where is\s+",
        r"^how does\s+",
        r"^why is\s+",
        r"^define\s+",
        r"^explain\s+",
        r"meaning of\s+"
    ]
    
    for pattern in patterns:
        if re.search(pattern, question_lower):
            return True
    
    return False

def search_internet(query: str) -> str:
    """Search the internet for real-time data"""
    try:
        # Try DuckDuckGo API (free, no API key required)
        url = "https://api.duckduckgo.com/"
        params = {
            "q": query,
            "format": "json",
            "no_html": 1,
            "skip_disambig": 1
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        # Extract relevant information
        result = data.get("AbstractText", "")
        if not result:
            result = data.get("Answer", "")
        if not result:
            result = data.get("Definition", "")
        
        if result:
            return result
        
        # If no result, return None
        return None
        
    except Exception as e:
        print(f"Internet search failed: {e}")
        return None

def get_gold_price() -> str:
    """Get current gold price from API"""
    try:
        url = "https://api.gold-api.com/price/XAU"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            price = data.get("price", 0)
            if price:
                return f"Current gold price: ${price:.2f} per ounce"
    except Exception as e:
        print(f"Gold price fetch failed: {e}")
    return None

def get_bitcoin_price() -> str:
    """Get current Bitcoin price from API"""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            price = data.get("bitcoin", {}).get("usd", 0)
            if price:
                return f"Current Bitcoin price: ${price:,.2f} USD"
    except Exception as e:
        print(f"Bitcoin price fetch failed: {e}")
    return None

def get_exchange_rate() -> str:
    """Get USD to INR exchange rate"""
    try:
        url = "https://api.exchangerate-api.com/v4/latest/USD"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            inr_rate = data.get("rates", {}).get("INR", 0)
            if inr_rate:
                return f"Current exchange rate: 1 USD = {inr_rate:.2f} INR"
    except Exception as e:
        print(f"Exchange rate fetch failed: {e}")
    return None

def get_weather(city: str = "New Delhi") -> str:
    """Get weather data"""
    try:
        # Using free weather API (no key required for basic)
        url = f"https://api.open-meteo.com/v1/forecast?latitude=28.61&longitude=77.23&current_weather=true"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            temp = data.get("current_weather", {}).get("temperature", 0)
            weather_code = data.get("current_weather", {}).get("weathercode", 0)
            
            # Map weather codes to descriptions
            weather_map = {
                0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy",
                3: "Overcast", 45: "Foggy", 48: "Depositing rime fog",
                51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
                61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
                71: "Slight snow fall", 73: "Moderate snow fall", 75: "Heavy snow fall",
                80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers"
            }
            description = weather_map.get(weather_code, "Unknown")
            
            return f"Current weather in Delhi: {description}, {temp:.1f}°C"
    except Exception as e:
        print(f"Weather fetch failed: {e}")
    return None

def handle_specific_queries(question: str) -> str:
    """Handle specific types of queries with specialized APIs"""
    question_lower = question.lower()
    
    # Gold price
    if "gold price" in question_lower or "price of gold" in question_lower:
        result = get_gold_price()
        if result:
            return f"{result}\n"
    
    # Bitcoin/Crypto price
    if "bitcoin" in question_lower or "btc" in question_lower:
        result = get_bitcoin_price()
        if result:
            return f"{result}\n"
    
    # Exchange rate
    if "exchange rate" in question_lower or "dollar to rupee" in question_lower or "usd to inr" in question_lower:
        result = get_exchange_rate()
        if result:
            return f" {result}\n\n"
    
    # Weather
    if "weather" in question_lower:
        # Extract city name
        city_match = re.search(r"weather in (\w+)", question_lower)
        city = city_match.group(1) if city_match else "Delhi"
        result = get_weather(city)
        if result:
            return f"{result}\n"
    
    return None

def check_if_answer_in_context(question: str, context: str) -> bool:
    """Check if the context actually contains relevant information"""
    # Extract key terms from question
    question_lower = question.lower()
    key_terms = re.findall(r'\b\w+\b', question_lower)
    
    # Remove common words
    stop_words = ['what', 'is', 'are', 'the', 'of', 'to', 'for', 'in', 'on', 'at', 'with', 'by']
    key_terms = [term for term in key_terms if term not in stop_words and len(term) > 2]
    
    # Check if any key term appears in context
    context_lower = context.lower()
    for term in key_terms[:5]:  # Check first 5 key terms
        if term in context_lower:
            return True
    return False

def query_rag(question: str, domain: str = "ALL", n_results: int = 5) -> str:
    """Query RAG with domain filtering and internet fallback"""
    
    # FIRST: Check if it's a real-time question that should bypass RAG
    if is_real_time_question(question):
        # Try specific handlers first
        specific_result = handle_specific_queries(question)
        if specific_result:
            return specific_result
        
        # Try general internet search for real-time data
        internet_result = search_internet(question)
        if internet_result:
            personality = DOMAIN_PERSONALITIES.get(domain, DOMAIN_PERSONALITIES["ALL"])
            prompt = f"""{personality}

Based on the following information from the internet, answer the question.
Provide a clear and concise answer. If the information is from a specific date, mention that.

Information:
{internet_result}

Question: {question}

Answer:"""
            
            try:
                response = groq_client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {"role": "user", "content": prompt}
                        ],
                        max_tokens=500
                        )
                return f"{response.choices[0].message.content}\n\n[Data sourced from internet]"
            except Exception as e:
                return f"{internet_result}\n\n[Data sourced from internet]"
    
    # SECOND: Try RAG with articles
    filter_dict = {}
    if domain != "ALL":
        filter_dict = {"domain": domain}

    results = collection.query(
        query_texts=[question],
        n_results=n_results * 2 if domain != "ALL" else n_results,
        where=filter_dict if filter_dict else None
    )

    chunks = results["documents"][0] if results["documents"] else []
    sources = results["metadatas"][0] if results["metadatas"] else []

    # If we have chunks, check if they actually answer the question
    if chunks:
        # Build context to check relevance
        context = ""
        for i, (chunk, meta) in enumerate(zip(chunks[:n_results], sources[:n_results])):
            context += f"\n[Article {i+1}]\n{chunk[:500]}\n"
        
        # Check if context is relevant to the question
        is_relevant = check_if_answer_in_context(question, context)
        
        if is_relevant:
            # Use RAG since articles are relevant
            context = ""
            for i, (chunk, meta) in enumerate(zip(chunks[:n_results], sources[:n_results])):
                context += f"\n[Article {i+1} — {meta.get('source', 'unknown')} — {meta.get('domain', 'GEO')} — {meta.get('published_at', '')}]\n{chunk}\n"

            personality = DOMAIN_PERSONALITIES.get(domain, DOMAIN_PERSONALITIES["ALL"])

            prompt = f"""{personality}

Answer the question below using ONLY the provided articles.
Always relate your answer to India's perspective and interests.
Keep the answer concise — 3 to 5 sentences.

Articles:
{context}

Question: {question}

Answer:"""

            try:
                response = groq_client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=[
                        {"role": "user", "content": prompt}
                        ],
                        max_tokens=500
                        )
                return response.choices[0].message.content
            except Exception as e:
                return f"Error querying LLM: {e}"
    
    # THIRD: If RAG didn't work, try internet search
    internet_result = search_internet(question)
    if internet_result:
        personality = DOMAIN_PERSONALITIES.get(domain, DOMAIN_PERSONALITIES["ALL"])
        prompt = f"""{personality}

Based on the following information from the internet, answer the question.
Provide a clear and concise answer.

Information:
{internet_result}

Question: {question}

Answer:"""
        
        try:
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "user", "content": prompt}
                    ],
                    max_tokens=500
                    )
            return f"{response.choices[0].message.content}\n\n[Data sourced from internet]"
        except Exception as e:
            return f"{internet_result}\n\n[Data sourced from internet]"
    
    # FOURTH: Check if it's general knowledge
    if is_general_knowledge(question):
        personality = DOMAIN_PERSONALITIES.get(domain, DOMAIN_PERSONALITIES["ALL"])
        prompt = f"""{personality}

Answer the following question based on your general knowledge.
Always relate your answer to India's perspective and interests when relevant.
If you're not sure, be honest and say so.

Question: {question}

Answer:"""
        
        try:
            response = groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[
                    {"role": "user", "content": prompt}
                    ],
                    max_tokens=500
                    )
            return f"{response.choices[0].message.content}\n\n[Based on general knowledge]"
        except Exception as e:
            return f"Error: {e}"
    
    # FIFTH: Final fallback
    domain_name = domain if domain != "ALL" else "ALL"
    return f"""I couldn't find specific information about this in the GOE knowledge base or on the internet for domain '{domain_name}'.

Suggestions:
1. Try switching to a different domain (e.g., 'ALL')
2. Refine your question to be more specific
3. Try asking about a different topic
4. Use the sync button to fetch latest articles

Question: {question}"""

def index_articles(limit: int = None):
    """Index articles into ChromaDB with domain metadata"""
    conn = get_connection()
    cursor = conn.cursor()
    
    if limit:
        cursor.execute("""
            SELECT id, title, body, source, published_at, domain 
            FROM articles 
            ORDER BY ingested_at DESC 
            LIMIT ?
        """, (limit,))
    else:
        cursor.execute("SELECT id, title, body, source, published_at, domain FROM articles")
    
    articles = cursor.fetchall()
    conn.close()

    print(f"Indexing {len(articles)} articles into ChromaDB...")

    existing = set(collection.get()["ids"]) if collection.get()["ids"] else set()

    docs, ids, metas = [], [], []
    for article in articles:
        doc_id = f"article_{article['id']}"
        if doc_id in existing:
            continue
        text = f"{article['title']}. {article['body']}"
        docs.append(text)
        ids.append(doc_id)
        metas.append({
            "source": article["source"],
            "published_at": article["published_at"],
            "domain": article["domain"] or "GEO",
            "title": article["title"]
        })

    if docs:
        collection.add(documents=docs, ids=ids, metadatas=metas)
        print(f"Added {len(docs)} new articles to ChromaDB.")
    else:
        print("No new articles to index.")

    return {"indexed": len(docs), "total": len(articles)}

def get_domain_stats():
    """Get indexing statistics per domain"""
    all_ids = collection.get()["ids"] if collection.get()["ids"] else []
    
    if not all_ids:
        return {}
    
    results = collection.get()
    domains = {}
    
    for meta in results["metadatas"]:
        domain = meta.get("domain", "GEO")
        domains[domain] = domains.get(domain, 0) + 1
    
    return domains

def search_by_domain(domain: str, query: str = "", n_results: int = 10):
    """Search for articles in a specific domain"""
    if query:
        results = collection.query(
            query_texts=[query],
            n_results=n_results,
            where={"domain": domain}
        )
    else:
        results = collection.get(
            where={"domain": domain},
            limit=n_results
        )
    
    return {
        "documents": results["documents"] if results.get("documents") else [],
        "metadatas": results["metadatas"] if results.get("metadatas") else [],
        "ids": results["ids"] if results.get("ids") else []
    }

if __name__ == "__main__":
    index_articles()
    
    print("\n--- Testing RAG with Internet Fallback ---")
    
    test_questions = [
        "What is the current price of Bitcoin?",
        "What is the weather in Delhi today?",
        "What is the capital of India?",
        "What are the latest defense developments?"
    ]
    
    for q in test_questions:
        print(f"\nQ: {q}")
        print(f"A: {query_rag(q)}")