# src/cache.py

import requests_cache
from config.settings import CACHE_NAME, CACHE_BACKEND, CACHE_EXPIRE

def get_cached_session():
    session = requests_cache.CachedSession(CACHE_NAME, backend=CACHE_BACKEND, expire_after=CACHE_EXPIRE)
    return session
