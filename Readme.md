# 🌐 Mini GOE — Global Ontology Engine (India's POV)

A working intelligence system that automatically collects news across multiple domains, extracts entities, builds a knowledge graph, and answers geopolitical & strategic questions from India's perspective — using its own ingested data, with a live internet fallback when needed.

> Built as a minimal working version of a full-scale Global Ontology Engine. Same core concepts (ingest → extract → graph → retrieve → answer), smaller scale.

---

## 🔴 Live Demo

| Part | Link |
|---|---|
| Dashboard (frontend) | https://mini-goe.vercel.app |
| API (backend) | https://mini-goe.onrender.com/api/health |

> Note: the backend is hosted on Render's free tier, so the first request after inactivity can take 30–50 seconds to "wake up" — this is normal cold-start behaviour, not a bug.

---

## What it does

- Automatically fetches news from The Hindu, Reuters, PIB, NDTV, Times of India, Economic Times, Hindustan Times, Inc42 and more, sorted into **6 domains**: `GEO`, `DEFENSE`, `TECH`, `CLIMATE`, `ECONOMICS`, `SOCIETY`
- Extracts people, countries, and organizations from every article using spaCy NLP
- Builds a knowledge graph in Neo4j — which entities co-occur in the same articles, and how strongly
- Answers natural-language questions (e.g. *"What are the latest India–Pakistan developments?"*) using its own ingested articles, with a domain-specific analyst persona (Defense analyst, Tech analyst, Climate analyst, etc.) powered by Groq's Llama 3.3 70B
- Displays everything on a live dashboard with a resizable layout: live news sidebar, world map, India-specific map, entity graph panel, and an AI console to chat with

---

## How it works — the pipeline (workflow)

```
┌─────────────────────┐
│  1. INGESTION        │   ml/ingestion/fetch.py
│  RSS feeds → DB      │   Pulls ~15 RSS feeds tagged by domain, dedupes by
│                      │   URL fingerprint (sha256), stores raw articles
└──────────┬───────────┘
           │  (runs every 4 hrs via scheduler.py)
           ▼
┌─────────────────────┐
│  2. NER EXTRACTION   │   ml/nlp/ner.py
│  Articles → Entities │   spaCy (en_core_web_sm) pulls PERSON, GPE, ORG,
│                      │   EVENT, NORP out of unprocessed articles
└──────────┬───────────┘
           ▼
┌─────────────────────┐
│  3. GRAPH BUILD      │   ml/graph/graph.py
│  Entities → Neo4j    │   Any two entities that appear in the same article
│                      │   get a CO_MENTIONED relationship in Neo4j —
│                      │   this is what powers the "who is connected to
│                      │   whom" graph panel on the dashboard
└──────────┬───────────┘
           ▼
┌─────────────────────┐
│  4. INDEXING         │   ml/rag/rag.py
│  Articles            │   
└──────────┬───────────┘
           ▼
┌─────────────────────┐
│  5. QUERY / ANSWER   │   backend/app/api/routes.py
│  Question → Answer   │   POST /api/query pulls the latest relevant
│                      │   articles for the selected domain, builds a
│                      │   context prompt with a domain-specific analyst
│                      │   persona, and asks Groq's llama-3.3-70b-versatile
│                      │   to answer — always framed around India's
│                      │   interests
└──────────┬───────────┘
           ▼
┌─────────────────────┐
│  6. DASHBOARD        │   frontend/ (React + Vite)
│  Answer → UI         │   AIConsole.jsx shows the chat, GraphPanel.jsx
│                      │   shows entity connections, WorldMap.jsx /
│                      │   IndiaPanel.jsx visualize where the news is
│                      │   happening, Sidebar.jsx shows the live feed
└─────────────────────┘
```

### The "smart answer" logic (`ml/rag/rag.py`)

The RAG layer doesn't just blindly search articles — it triages every question first:

1. **Is it a real-time question?** (price, weather, "latest", "today", crypto, exchange rate) → skip the article DB, hit a live API (gold-api.com, CoinGecko, Open-Meteo, exchangerate-api.com) or a DuckDuckGo lookup, then let Groq phrase the final answer.
2. **Otherwise, search DB** for the most relevant ingested articles (filtered by domain if one is selected), and check if they're actually relevant to the question.
3. **If relevant articles are found** → build a context prompt with a domain-specific analyst persona and ask Groq to answer using only those articles.
4. **If nothing relevant is found** → try one more live internet search.
5. **If it's a general-knowledge question** (what is / who is / define / explain) → answer straight from the LLM's own knowledge, labelled as such.



---

## Project structure

```
mini_goe/
│
├── backend/                      # FastAPI server (deployed on Render)
│   ├── app/
│   │   ├── main.py               # App entry point, CORS setup
│   │   ├── api/
│   │   │   └── routes.py         # All API endpoints
│   │   ├── core/
│   │   │   └── config.py         # Settings, loads from .env
│   │   └── services/
│   │       ├── db/
│   │       │   └── database.py   # Postgres (Supabase) article queries
│   │       └── ner_services.py   # Neo4j entity/graph queries
│   └── requirements.txt
│
├── frontend/                     # React + Vite dashboard (deployed on Vercel)
│   ├── src/
│   │   ├── components/
│   │   │   ├── WorldMap.jsx      # World map with layers
│   │   │   ├── IndiaPanel.jsx    # India-specific map view
│   │   │   ├── GraphPanel.jsx    # Neo4j entity connection graph
│   │   │   ├── AIConsole.jsx     # Chat interface → backend /api/query
│   │   │   ├── Sidebar.jsx       # Live news feed
│   │   │   └── Navbar.jsx        # Domain switcher (ALL/GEO/DEFENSE/...)
│   │   ├── pages/
│   │   │   └── Dashboard.jsx     # Resizable panel layout
│   │   ├── config.js             # Reads VITE_API_URL
│   │   └── styles/globals.css
│   ├── .env.production           # VITE_API_URL for the deployed backend
│   ├── package.json
│   └── vite.config.js
│
├── ml/                            # ML / data pipeline
│   ├── ingestion/
│   │   ├── fetch.py               # RSS ingestion across 6 domains
│   │   ├── store.py               # Postgres connection + table setup
│   │   ├── migrate.py             # One-off local SQLite → Postgres migration
│   │   ├── scheduler.py           # Runs fetch.py every 4 hours
│   │   └── test_feeds.py          # Sanity-check RSS feeds are alive
│   ├── nlp/
│   │   └── ner.py                 # spaCy entity extraction
│   ├── graph/
│   │   └── graph.py               # Builds CO_MENTIONED graph in Neo4j
│   ├── rag/
│   │   └── rag.py                 # DB indexing + Groq-based Q&A + live fallbacks
│   └── requirements.txt
│
├── data/                          # Auto-created locally, never committed
│   └── chroma_db/                 # Local vector store
│
├── .env                           # Credentials (never commit)
└── .gitignore
```
<img width="1600" height="1007" alt="WhatsApp Image 2026-07-13 at 8 10 26 PM" src="https://github.com/user-attachments/assets/0d7e7742-b077-4cdb-b0be-331da2a64a9c" />


---

## Tech stack

| Layer | Technology |
|---|---|
| News ingestion | Python, feedparser |
| Database | PostgreSQL (Supabase) — articles & entities |
| Entity extraction | spaCy `en_core_web_sm` |
| Knowledge graph | Neo4j (AuraDB in production) |
| Vector store 
| LLM | Groq — `llama-3.3-70b-versatile` |
| Live data fallback | DuckDuckGo Instant Answer, CoinGecko, Open-Meteo, exchangerate-api, gold-api |
| Backend API | FastAPI + uvicorn (hosted on Render) |
| Frontend | React 19 + Vite (hosted on Vercel) |
| Maps | Leaflet-style world/India panels |
| Charts | Recharts |

---

## Prerequisites (for local development)

| Tool | Version | Download |
|---|---|---|
| Python | 3.12.x | https://www.python.org/downloads/ |
| Node.js | 18+ | https://nodejs.org |
| Neo4j Desktop / AuraDB | Latest | https://neo4j.com/download |
| PostgreSQL / Supabase project | Latest | https://supabase.com |
| Groq API key | — | https://console.groq.com |

> **Important:** Use Python 3.12. spaCy does not yet reliably support 3.13+.

---

## Setup — step by step

### 1. Clone the repo

```bash
git clone https://github.com/equating1729/mini_goe.git
cd mini_goe
```

### 2. Create a `.env` file in the root folder

```
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password

DATABASE_URL=postgresql://user:password@host:5432/dbname
GROQ_API_KEY=your_groq_api_key
```

> This file is gitignored. Never commit it.

### 3. Set up Neo4j

1. Open Neo4j Desktop (or create a free AuraDB instance)
2. Create/start a local DBMS (or your AuraDB instance) and note the connection URI
3. Put the URI, user, and password into `.env`

### 4. Set up the database (Postgres / Supabase)

1. Create a free project at https://supabase.com (or use any Postgres instance)
2. Copy the connection string into `DATABASE_URL` in `.env`
3. Run the table setup:

```bash
cd ml
python -m venv venv
source venv/bin/activate          # Mac/Linux
# venv\Scripts\activate           # Windows

pip install -r requirements.txt
python -m spacy download en_core_web_sm

python ingestion/store.py         # creates the articles + entities tables
```

### 5. Run the ML pipeline

```bash
# Step 1: Fetch articles from all 6 domains
python ingestion/fetch.py

# Step 2: Extract entities (PERSON, GPE, ORG, EVENT, NORP)
python nlp/ner.py

# Step 3: Build the knowledge graph in Neo4j
python graph/graph.py

# Step 4: Index articles into ChromaDB + test the RAG pipeline
python rag/rag.py
```

To keep ingestion running automatically every 4 hours:
```bash
python ingestion/scheduler.py
```

### 6. Set up the backend

```bash
cd ../backend
pip install -r requirements.txt

uvicorn app.main:app --reload --port 8000
```

Verify it works:
```
http://localhost:8000/api/health
→ {"status": "GOE backend running", "version": "2.0.0"}

http://localhost:8000/api/stats
→ {"total_articles": ..., "total_entities": ..., "by_source": [...], "by_domain": [...]}
```

### 7. Set up the frontend

```bash
cd ../frontend
npm install
npm run dev
```

For local dev, point the frontend at your local backend by setting `VITE_API_URL=http://localhost:8000/api` in a `.env` file inside `frontend/` (the checked-in `.env.production` points at the deployed Render backend and is only used for production builds).

Open in browser:
```
http://localhost:5173
```

---

## Running the full system locally

You need up to 3 terminals running at the same time:

| Terminal | Command | What it does |
|---|---|---|
| Terminal 1 | `cd ml && python ingestion/scheduler.py` | Fetches news every 4 hours |
| Terminal 2 | `cd backend && uvicorn app.main:app --reload --port 8000` | API server |
| Terminal 3 | `cd frontend && npm run dev` | Dashboard |

---

## API endpoints

| Endpoint | Method | What it returns |
|---|---|---|
| `/api/health` | GET | Server status |
| `/api/domains` | GET | List of available domains (ALL, GEO, DEFENSE, TECH, CLIMATE) with icons/descriptions |
| `/api/stats` | GET | Article + entity counts, optionally filtered by `domain` |
| `/api/articles` | GET | Latest articles, optional `limit` and `domain` filters |
| `/api/query` | POST | LLM answer grounded in the latest articles for the given domain |
| `/api/graph/entities` | GET | Top entities by number of connections, optional `domain` filter |
| `/api/graph/entity/{name}` | GET | Connections for a single entity |
| `/api/index` | POST | Re-index articles into ChromaDB (runs the full RAG indexing pipeline) |

Example query:
```bash
curl -X POST https://mini-goe.onrender.com/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the latest India-China developments?", "domain": "GEO"}'
```

---

## Common issues

**`ModuleNotFoundError: No module named 'app'`**
You are not in the `backend` folder — run `cd backend` first.

**`ServiceUnavailable: Unable to retrieve routing information`**
Neo4j is not running/reachable — start your local instance or check your AuraDB URI.

**`psycopg2.OperationalError` on backend start**
`DATABASE_URL` in `.env` is missing or wrong — double-check your Supabase/Postgres connection string.

**`GROQ_API_KEY` errors on `/api/query`**
Get a free key from https://console.groq.com and add it to `.env`.

**spaCy install fails on Python 3.13+**
Use Python 3.12.

**Frontend shows a connection error**
Make sure the backend is running and `VITE_API_URL` in your frontend `.env` points to it correctly.

**First request to the live demo is slow**
The Render free tier spins down when idle — the first request after a while can take 30–50 seconds.

---

## .gitignore

Make sure your `.gitignore` includes:

```
.env
data/
venv/
__pycache__/
*.pyc
node_modules/
.DS_Store
```

---

## What this is based on

This is a minimal working version of a **Global Ontology Engine** — a system that ingests multi-domain data, extracts structured knowledge, builds a relationship graph, and enables AI-powered querying grounded in that knowledge, with a live fallback for anything time-sensitive. A full-scale version would add streaming ingestion (Kafka), GNN-based relation inference, satellite/imagery data, and much broader domain coverage. This version demonstrates the same core pipeline — ingest → extract → graph → retrieve → answer — at a scale that runs on a laptop and deploys for free.
