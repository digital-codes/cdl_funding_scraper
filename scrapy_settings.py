scrapy_settings = {
    "HTTPERROR_ALLOW_ALL": False,
    "BOT_NAME": "cdl_awo_funding_crawler",
    "LOG_LEVEL": "INFO",
    "CONCURRENT_REQUESTS": 6,
    "LOGSTATS_INTERVAL": "5.0",
    # https://docs.scrapy.org/en/latest/topics/practices.html#avoiding-getting-banned
    "COOKIES_ENABLED": False,
    "DOWNLOAD_DELAY": 0.8,
    "RANDOMIZE_DOWNLOAD_DELAY": True,
    "RETRY_TIMES": 3,
}
