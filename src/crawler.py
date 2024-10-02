# src/crawler.py

import requests
from bs4 import BeautifulSoup
import time
from src.parser import parse_page
from src.cache import get_cached_session
from src.exporter import save_to_json
import logging
from config.settings import BASE_URL, MAX_PAGES, SLEEP_INTERVAL, LOG_FILE

# Logging konfigurieren
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def crawl():
    session = get_cached_session()
    
    # Phase 1: Extrahieren der Links von den Seiten mit den Suchergebnissen
    all_links = extract_links(session)

    # Phase 2: Iterieren über die extrahierten Links und Inhalte extrahieren
    extracted_data = []
    for idx, link in enumerate(all_links, start=1):
        logging.info(f'Abrufen von Seite {idx}: {link}')
        response = session.get(link)
        if response.from_cache:
            logging.info(f'Seite {idx} wird aus dem Cache geladen: {link}')
        else:
            logging.info(f'Seite {idx} wird neu abgerufen: {link}')
            time.sleep(SLEEP_INTERVAL)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        page_data = parse_page(soup, link)  # Übergabe der URL an parse_page
        extracted_data.append(page_data)
    
    # Daten speichern
    save_to_json(extracted_data)

def extract_links(session):
    """
    Diese Funktion extrahiert alle Links von den Suchergebnisseiten.
    """
    current_url = BASE_URL
    page_number = 1
    all_links = []

    while current_url and page_number <= MAX_PAGES:
        response = session.get(current_url)
        if response.from_cache:
            logging.info(f'Seite {page_number} wird aus dem Cache geladen: {current_url}')
        else:
            logging.info(f'Abrufen von Seite {page_number}: {current_url}')
            time.sleep(SLEEP_INTERVAL)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Links auf der aktuellen Seite extrahieren
        links = get_links_from_page(soup)
        all_links.extend(links)
        
        # Nächste Seite bestimmen
        current_url = get_next_page_url(soup)
        page_number += 1

    return all_links

def get_links_from_page(soup):
    """
    Extrahiert alle relevanten Links von einer Suchergebnisseite.
    """
    links = []
    for a in soup.find_all('a', title="Öffnet die Einzelsicht"):
        href = a.get('href')
        if href:
            links.append('https://www.foerderdatenbank.de/' + href)
    return links

def get_next_page_url(soup):
    """
    Findet den Link zur nächsten Suchergebnisseite.
    """
    next_page = soup.find('a', class_='forward button', title="SucheSeite")
    if next_page:
        return 'https://www.foerderdatenbank.de/' + next_page.get('href')
    return None

if __name__ == "__main__":
    crawl()
