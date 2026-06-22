# 🌐 Mini GOE — Global Ontology Engine (India's POV)

A working intelligence system that automatically collects news, extracts entities, builds a knowledge graph, and answers geopolitical questions from India's perspective — using your own data.

> Built as a minimal working version of a full-scale Global Ontology Engine. Same concepts, smaller scale.

---

## What it does

- Automatically fetches news from The Hindu, NDTV, BBC India every 4 hours
- Extracts people, countries, and organizations from every article using NLP
- Builds a knowledge graph in Neo4j — who is connected to whom, and how strongly
- Answers questions like *"What are the latest India-Pakistan developments?"* using your own ingested articles — not a generic AI
- Displays everything on a live dashboard with a world map, India map, and AI console

---

## Project structure

```
mini_goe/
│
├── backend/                  # FastAPI server
│   ├── app/
│   │   ├── main.py           # App entry point, CORS setup
│   │   ├── api/
│   │   │   └── routes.py     # API endpoints
│   │   ├── core/
│   │   │   └── config.py     # Settings, loads from .env
│   │   └── services/
│   │       ├── db/
│   │       │   └── database.py   # SQLite queries
│   │       └── ner_services.py   # Neo4j graph queries
│   └── requirements.txt
│
├── frontend/                 # React + Vite app
│   ├── src/
│   │   ├── components/
│   │   │   ├── WorldMap.jsx      # Leaflet world map with layers
│   │   │   ├── IndiaPanel.jsx    # India-specific map
│   │   │   ├── AIConsole.jsx     # Chat interface → backend RAG
│   │   │   ├── Sidebar.jsx       # Live news feed
│   │   │   ├── Navbar.jsx
│   │   │   └── Footer.jsx
│   │   ├── pages/
│   │   │   └── Dashboard.jsx     # Main layout
│   │   └── styles/
│   │       └── globals.css
│   ├── package.json
│   └── vite.config.js
│
├── ml/                       # ML pipeline
│   ├── ingestion/
│   │   ├── fetch.py          # RSS ingestion
│   │   ├── store.py          # SQLite setup
│   │   └── scheduler.py      # Runs every 4 hours
│   ├── nlp/
│   │   └── ner.py            # spaCy entity extraction
│   ├── graph/
│   │   └── graph.py          # Neo4j knowledge graph builder
│   ├── rag/
│   │   └── rag.py            # ChromaDB + Ollama RAG pipeline
│   └── requirements.txt
│
├── data/                     # Auto-created, do not commit
│   ├── goe.db                # SQLite database
│   └── chroma_db/            # Vector store
│
├── .env                      # Credentials (do not commit)
└── .gitignore
```

---

## Prerequisites

Make sure these are installed before starting:

| Tool | Version | Download |
|---|---|---|
| Python | 3.12.x | https://www.python.org/downloads/ |
| Node.js | 18+ | https://nodejs.org |
| Neo4j Desktop | Latest | https://neo4j.com/download |
| Ollama | Latest | https://ollama.com/download |

> **Important:** Use Python 3.12. Python 3.13+ is not yet supported by spaCy and several other dependencies.

---

## Setup — step by step

### 1. Clone the repo

```bash
git clone https://github.com/equating1729/mini_goe.git
cd mini_goe
```

---

### 2. Create `.env` file in root folder

Create a file called `.env` in the `mini_goe` root folder:

```
NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password
```

> This file is gitignored. Never commit it.

---

### 3. Set up Neo4j

1. Open **Neo4j Desktop**
2. Create a new **Local DBMS** — name it `mini-goe`
3. Set a password — put it in `.env` as `NEO4J_PASSWORD`
4. Click **Start** — the green dot should appear

---

### 4. Install Ollama + model

```bash
# After installing Ollama from https://ollama.com/download
ollama pull llama3.2
```

Verify it works:
```bash
ollama run llama3.2
# Type /bye to exit
```

---

### 5. Set up ML pipeline

```bash
cd ml

# Create virtual environment (recommended)
python3.12 -m venv venv
source venv/bin/activate          # Mac/Linux
# venv\Scripts\activate           # Windows

pip install feedparser schedule spacy chromadb ollama
python -m spacy download en_core_web_sm
```

---

### 6. Run the ML pipeline

Run these in order — each step depends on the previous:

```bash
# Step 1: Initialize database
python ingestion/store.py

# Step 2: Fetch articles
python ingestion/fetch.py

# Step 3: Extract entities
python nlp/ner.py

# Step 4: Build knowledge graph (Neo4j must be running)
python graph/graph.py

# Step 5: Index articles + test RAG
python rag/rag.py
```

After Step 2 you should see ~500+ articles fetched. After Step 3 you should see ~2000+ entities extracted.

To keep ingestion running automatically every 4 hours:
```bash
python ingestion/scheduler.py
```

---

### 7. Set up backend

```bash
cd ../backend

pip install fastapi uvicorn pydantic-settings python-dotenv neo4j
```

Start the server:
```bash
uvicorn app.main:app --reload --port 8000
```

Verify it works — open in browser:
```
http://localhost:8000/api/health
→ {"status": "GOE backend running"}

http://localhost:8000/api/stats
→ {"total_articles": 542, "total_entities": 2248, ...}
```

---

### 8. Set up frontend

```bash
cd ../frontend

npm install
npm run dev
```

Open in browser:
```
http://localhost:5173
```

---

## Running the full system

You need 3 terminals running at the same time:

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
| `/api/stats` | GET | Article + entity counts |
| `/api/articles` | GET | Latest 20 articles |
| `/api/query` | POST | RAG answer from your data |
| `/api/graph/entities` | GET | Top entities by connections |
| `/api/graph/entity/{name}` | GET | Connections for one entity |
| `/api/index` | POST | Re-index articles into ChromaDB |

Example query:
```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What are the latest India-China developments?"}'
```

---

## Tech stack

| Layer | Technology |
|---|---|
| News ingestion | Python, feedparser, SQLite |
| Entity extraction | spaCy en_core_web_sm |
| Knowledge graph | Neo4j |
| Vector search | ChromaDB |
| LLM | Ollama (llama3.2) — runs locally, free |
| RAG orchestration | LangChain |
| Backend API | FastAPI, uvicorn |
| Frontend | React, Vite |
| Maps | Leaflet.js |

---

## Common issues

**`ModuleNotFoundError: No module named 'app'`**
You are not in the `backend` folder. Run `cd backend` first.

**`ServiceUnavailable: Unable to retrieve routing information`**
Neo4j is not running. Open Neo4j Desktop and click Start on your instance.

**`SSL error during pip install`**
Add `--trusted-host pypi.org --trusted-host files.pythonhosted.org` to your pip command.

**spaCy install fails on Python 3.13+**
Use Python 3.12. spaCy does not support 3.13+ yet.

**Ollama model not found**
Run `ollama pull llama3.2` before starting the backend.

**Frontend shows "Connection to GOE backend failed"**
Make sure the backend is running on port 8000 and Neo4j is started.

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

## Architecture overview

```
News Sources (RSS)
      ↓ fetch.py
SQLite (articles)
      ↓ ner.py
SQLite (entities)
      ↓ graph.py
Neo4j (knowledge graph)
      ↓ rag.py
ChromaDB (embeddings)
      ↓
FastAPI (/api/query)
      ↓
React (AIConsole.jsx)
      ↓
Answer on dashboard
```

---

## What this is based on

This is a minimal working version of a **Global Ontology Engine** — a system that ingests multi-domain data, extracts structured knowledge, and enables AI-powered querying. The full version would include Kafka streaming, GNN-based relation inference, satellite data, and multi-domain coverage. This version demonstrates the same core pipeline at laptop scale.
