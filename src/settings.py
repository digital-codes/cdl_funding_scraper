# config/settings.py

BASE_URL = "https://www.foerderdatenbank.de/SiteGlobals/FDB/Forms/Suche/Foederprogrammsuche_Formular.html?filterCategories=FundingProgram"
CACHE_NAME = "data/cache/foerderdatenbank_cache"
CACHE_BACKEND = "sqlite"
CACHE_EXPIRE = None
MAX_RESULTS_PAGES = 5
SLEEP_INTERVAL = 30  # Sekunden
OUTPUT_FILE = "data/output.json"
LOG_FILE = "logs/crawler.log"
