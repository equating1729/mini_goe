import sqlite3
import spacy
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../ingestion"))
from store import get_connection

nlp = spacy.load("en_core_web_sm")

ENTITY_TYPES = ["PERSON", "GPE", "ORG", "EVENT", "NORP"]

def extract_entities(text):
    if not text:
        return []
    doc = nlp(text)
    entities = []
    for ent in doc.ents:
        if ent.label_ in ENTITY_TYPES:
            entities.append({
                "text": ent.text.strip(),
                "label": ent.label_
            })
    return entities

def run_ner():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, title, body FROM articles
        WHERE is_processed = 0
    """)
    articles = cursor.fetchall()

    print(f"Processing {len(articles)} articles...")

    for article in articles:
        article_id = article["id"]
        text = f"{article['title']}. {article['body']}"

        entities = extract_entities(text)

        cursor.execute("""
            UPDATE articles
            SET is_processed = 1
            WHERE id = ?
        """, (article_id,))

        for ent in entities:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id INTEGER,
                    text       TEXT,
                    label      TEXT,
                    FOREIGN KEY (article_id) REFERENCES articles(id)
                )
            """)
            cursor.execute("""
                INSERT INTO entities (article_id, text, label)
                VALUES (?, ?, ?)
            """, (article_id, ent["text"], ent["label"]))

    conn.commit()
    conn.close()
    print("NER complete.")

if __name__ == "__main__":
    run_ner()