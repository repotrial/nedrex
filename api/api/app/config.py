import os
from functools import lru_cache

from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    mongodb_url = f"localhost:{os.getenv('MONGO_PORT')}"
    mongodb_db = "test"

    neo4j_url = "bolt://neo4j.repotrial.net:7687"

    results_dir = "./results"
    static_dir = "./static"

    bicon_env = os.getenv("BICON_PYENV")
    bicon_run = os.getenv("BICON_RUN")
    diamond_run = os.getenv("DIAMOND_RUN")
    trustrank_run = os.getenv("TRUSTRANK_RUN")
    closeness_run = os.getenv("CLOSENESS_RUN")
    must_run = os.getenv("MUST_RUN")

@lru_cache()
def get_settings():
    return Settings()
