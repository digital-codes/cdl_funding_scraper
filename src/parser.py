# src/parser.py

from bs4 import BeautifulSoup

def parse_page(soup: BeautifulSoup, url: str) -> dict:
    """
    Parst die Seite und extrahiert die benötigten Informationen.
    Fügt auch die Quell-URL zu den extrahierten Daten hinzu.
    
    :param soup: BeautifulSoup-Objekt der Seite
    :param url: Die URL der Seite, die geparst wird
    :return: Ein Dictionary mit den extrahierten Informationen und der Quell-URL
    """
    title = soup.find('h1', class_='title').get_text(strip=True)
    content = str(soup.find('div', class_='content'))

    details = {}
    for dt, dd in zip(soup.find_all('dt'), soup.find_all('dd')):
        key = dt.get_text(strip=True)
        
        # Behandlung von "Weiterführende Links"
        if key == "Weiterführende Links:":
            links = []
            for a in dd.find_all('a', href=True):
                link_text = a.get_text(strip=True)
                link_url = a['href']
                if not link_url.startswith('http'):
                    link_url = 'https://www.foerderdatenbank.de/' + link_url
                links.append({"text": link_text, "url": link_url})
            details[key] = links
        else:
            details[key] = dd.get_text(strip=True)

    return {
        'url': url,
        'title': title,
        'content': content,
        'details': details
    }
