import sqlite3
import sys
import os
from neo4j import GraphDatabase

sys.path.append(os.path.join(os.path.dirname(__file__), "../ingestion"))
from store import get_connection

URI = os.environ['URI']
AUTH = os.environ['AUTH']

def build_graph():
    conn = get_connection()
    cursor = conn.cursor()

    driver = GraphDatabase.driver(URI, auth=AUTH)

    cursor.execute("""
        SELECT e1.article_id, e1.text as entity1, e1.label as label1,
               e2.text as entity2, e2.label as label2
        FROM entities e1
        JOIN entities e2
        ON e1.article_id = e2.article_id
        AND e1.id < e2.id
    """)

    pairs = cursor.fetchall()
    print(f"Total pairs to insert: {len(pairs)}")

    with driver.session() as session:
        for pair in pairs:
            session.run("""
                MERGE (a:Entity {name: $name1, label: $label1})
                MERGE (b:Entity {name: $name2, label: $label2})
                MERGE (a)-[:CO_MENTIONED {article_id: $article_id}]->(b)
            """, {
                "name1": pair["entity1"],
                "label1": pair["label1"],
                "name2": pair["entity2"],
                "label2": pair["label2"],
                "article_id": pair["article_id"]
            })

    print("Graph built successfully!")
    driver.close()
    conn.close()

if __name__ == "__main__":
    build_graph()