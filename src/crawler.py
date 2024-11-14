# src/crawler.py

import logging
import time

import requests
from bs4 import BeautifulSoup

from config.settings import BASE_URL, LOG_FILE, MAX_RESULTS_PAGES, SLEEP_INTERVAL
from src.cache import get_cached_session
from src.exporter import save_to_json
from src.parser import parse_program_page

# Logging konfigurieren
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
)


def crawl():
    session = get_cached_session()

    # Phase 1: Extrahieren der Links von den Seiten mit den Suchergebnissen
    all_program_page_links = extract_program_page_links(session)
    # Phase 2: Iterieren über die extrahierten Links und Inhalte extrahieren
    extracted_data = []
    for idx, link in enumerate(all_program_page_links, start=1):
        logging.info(f"Abrufen von Förderprogrammseite {idx} von {len(all_program_page_links)}: {link}")
        response = session.get(link)
        if response.from_cache:
            logging.info(f"Förderprogrammseite {idx} wird aus dem Cache geladen: {link}")
        else:
            logging.info(f"Förderprogrammseite {idx} wird neu abgerufen: {link}")
            time.sleep(SLEEP_INTERVAL)

        program_page_data = parse_program_page(response.content, link)  # Übergabe der URL an parse_program_page
        extracted_data.append(program_page_data)

    # Daten speichern
    save_to_json(extracted_data)


def extract_program_page_links(session):
    """
    Diese Funktion extrahiert alle Links zu Förderprogrammseiten von den Suchergebnisseiten.
    """
    current_url = BASE_URL
    results_page_number = 1
    all_links = []

    while current_url and results_page_number <= MAX_RESULTS_PAGES:
        response = session.get(current_url)
        if response.from_cache:
            logging.info(
                f"Ergebnisseite {results_page_number} wird aus dem Cache geladen: {current_url}"
            )
        else:
            logging.info(f"Abrufen von Ergebnisseite {results_page_number}: {current_url}")
            time.sleep(SLEEP_INTERVAL)

        soup = BeautifulSoup(response.content, "html.parser")

        # Links auf der aktuellen Seite extrahieren
        links = get_links_from_results_page(soup)
        all_links.extend(links)

        # Nächste Seite bestimmen
        current_url = get_next_results_page_url(soup)
        results_page_number += 1

    return all_links


def get_links_from_results_page(soup):
    """
    Extrahiert alle relevanten Links von einer Suchergebnisseite.
    """
    links = []
    for a in soup.find_all("a", title="Öffnet die Einzelsicht"):
        href = a.get("href")
        if href:
            links.append("https://www.foerderdatenbank.de/" + href)
    return links


def get_next_results_page_url(soup):
    """
    Findet den Link zur nächsten Suchergebnisseite.
    """
    next_results_page = soup.find("a", class_="forward button", title="SucheSeite")
    if next_results_page:
        return "https://www.foerderdatenbank.de/" + next_results_page.get("href")
    return None


if __name__ == "__main__":
    crawl()
