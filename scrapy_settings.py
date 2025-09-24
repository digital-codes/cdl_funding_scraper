scrapy_settings = {
    "HTTPERROR_ALLOW_ALL": False,
    "BOT_NAME": "cdl_awo_funding_crawler",
    "LOG_LEVEL": "INFO",
    "CONCURRENT_REQUESTS": 7,
    "LOGSTATS_INTERVAL": 20.0,
    "COOKIES_ENABLED": True,
    "DOWNLOAD_DELAY": 0.7,
    "RANDOMIZE_DOWNLOAD_DELAY": True,
    "RETRY_TIMES": 5,
    "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 429, 522, 524],
    "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "DEFAULT_REQUEST_HEADERS": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    },
}
