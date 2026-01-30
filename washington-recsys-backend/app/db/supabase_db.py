from supabase import create_client, Client
from app.core.config import settings
from functools import lru_cache


@lru_cache()
def get_supabase_client() -> Client:
    """Get Supabase client for logging"""
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)


# Dependency for FastAPI
def get_supabase() -> Client:
    return get_supabase_client()
