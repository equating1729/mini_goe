from neo4j import GraphDatabase
from app.core.config import settings

driver = GraphDatabase.driver(
    settings.NEO4J_URI,
    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
)

def get_top_entities(limit: int = 50, domain: str = None):
    with driver.session() as session:
        if domain and domain != "ALL":
            result = session.run("""
                MATCH (e:Entity)-[r:CO_MENTIONED]-()
                WHERE r.domain = $domain
                WITH e, count(r) as connections
                RETURN e.name as name, e.label as label, connections
                ORDER BY connections DESC
                LIMIT $limit
            """, domain=domain, limit=limit)
        else:
            result = session.run("""
                MATCH (e:Entity)
                RETURN e.name as name, e.label as label,
                       size([(e)-[]-() | 1]) as connections
                ORDER BY connections DESC
                LIMIT $limit
            """, limit=limit)
        return [dict(r) for r in result]

def get_entity_connections(entity_name: str, domain: str = "ALL"):
    with driver.session() as session:
        if domain and domain != "ALL":
            result = session.run("""
                MATCH (a:Entity {name: $name})-[r:CO_MENTIONED]-(b:Entity)
                WHERE r.domain = $domain
                RETURN b.name as connected_to,
                       b.label as label,
                       count(r) as strength
                ORDER BY strength DESC
                LIMIT 20
            """, name=entity_name, domain=domain)
        else:
            result = session.run("""
                MATCH (a:Entity {name: $name})-[r]-(b:Entity)
                RETURN b.name as connected_to,
                       b.label as label,
                       count(r) as strength
                ORDER BY strength DESC
                LIMIT 20
            """, name=entity_name)
        return [dict(r) for r in result]