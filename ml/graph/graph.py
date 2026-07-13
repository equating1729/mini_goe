from dotenv import load_dotenv
load_dotenv()
import sqlite3
import sys
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../ingestion")
    )
)
from store import get_connection

URI = os.getenv('NEO4J_URI')
print("URI =", repr(URI))
USER=os.getenv("NEO4J_USER")
PASSWORD=os.getenv("NEO4J_PASSWORD")
AUTH=(USER,PASSWORD)



def build_graph():
    conn = get_connection()
    cursor = conn.cursor()

    driver = GraphDatabase.driver(URI, auth=AUTH)

    cursor.execute("""
        SELECT e1.article_id, e1.text as entity1, e1.label as label1,
               e2.text as entity2, e2.label as label2,
               a.domain as domain
        FROM entities e1
        JOIN entities e2
        ON e1.article_id = e2.article_id
        AND e1.id < e2.id
        JOIN articles a
        ON a.id = e1.article_id
    """)

    pairs = cursor.fetchall()
    print(f"Total pairs to insert: {len(pairs)}")

    # Batch size — AuraDB ke liye small batches
    BATCH_SIZE = 100

    with driver.session() as session:
        for i in range(0, len(pairs), BATCH_SIZE):
            batch = pairs[i:i + BATCH_SIZE]
            for pair in batch:
                try:
                    session.run("""
                        MERGE (a:Entity {name: $name1, label: $label1})
                        MERGE (b:Entity {name: $name2, label: $label2})
                        MERGE (a)-[:CO_MENTIONED {article_id: $article_id, domain: $domain}]->(b)
                    """, {
                        "name1": pair["entity1"],
                        "label1": pair["label1"],
                        "name2": pair["entity2"],
                        "label2": pair["label2"],
                        "article_id": pair["article_id"],
                        "domain": pair["domain"] or "GEO"
                    })
                except Exception as e:
                    print(f"  Skipping pair due to error: {e}")
                    continue

            print(f"  Inserted batch {i//BATCH_SIZE + 1}/{(len(pairs)//BATCH_SIZE) + 1}")

    print("Graph built successfully!")
    driver.close()
    conn.close()

if __name__ == "__main__":
    build_graph()