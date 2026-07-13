from pydantic_settings import BaseSettings
import os

ROOT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../..")
)

class Settings(BaseSettings):
    NEO4J_URI: str = "neo4j://127.0.0.1:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "neo4j"

    DATABASE_URL: str = ""
    DB_PATH: str = os.path.join(ROOT_DIR, "data/goe.db")
    CHROMA_PATH: str = os.path.join(ROOT_DIR, "data/chroma_db")

    GROQ_API_KEY: str = ""

    class Config:
        env_file = os.path.join(ROOT_DIR, ".env")
        env_file_encoding = "utf-8"

settings = Settings()
print(f"NEO4J_URI loaded: {settings.NEO4J_URI}")
print(f"DATABASE_URL loaded: {'yes' if settings.DATABASE_URL else 'no'}")