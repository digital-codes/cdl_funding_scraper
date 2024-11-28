# main.py

import logging

from lib.crawler import crawl

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.StreamHandler())
    crawl()
