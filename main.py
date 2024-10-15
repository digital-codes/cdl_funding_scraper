# main.py

import logging

from src.crawler import crawl

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.StreamHandler())
    crawl()
