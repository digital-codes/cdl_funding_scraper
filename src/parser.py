# src/parser.py
import lxml.html
import hashlib


def parse_program_page(html_bytes: bytes, url: str) -> dict:
    """
    Parst die Seite und extrahiert die benötigten Informationen.
    Fügt auch die Quell-URL zu den extrahierten Daten hinzu.

    :param html_bytes: bytes-Repräsentation des HTMLs
    :param url: Die URL der Seite, die geparst wird
    :return: Ein Dictionary mit den extrahierten Informationen und der Quell-URL
    """
    tree = lxml.html.fromstring(html_bytes)

    # title des Förderprogramms
    title = tree.xpath("//h1[@class='title']")[0].text_content().strip()

    articles = {}
    article_nodes = tree.xpath("//div[@class='content']//article")
    if (len(article_nodes) > 0):
        for article in article_nodes:
            tab_id = article.attrib.get("id")
            if tab_id:
                tab_name = (
                    tree.xpath(f'//h2[@data-toggleid="{tab_id}"]')[0].text_content().strip()
                )
            else:
                node = article.getprevious()
                assert node.tag == "h2"
                tab_name = node.text_content().strip()
            content = lxml.html.tostring(article, encoding="unicode")
            if tab_name in articles:
                raise Exception(f"Duplicate tab name: {tab_name}")
            articles[tab_name] = content
    else:
        # this page does not have "tabs" - very hacky workaround
        content_node = tree.xpath("//main/div[@class='jumbotron']/following-sibling::div/div[@class='content']")[0]
        content = lxml.html.tostring(content_node, encoding="unicode")
        # quickfix : construct artificial articles object with "Kurzzusammenfassung" 
        articles = [{"Kurzzusammenfassung": content}]

    details = {}
    dt_elements = tree.xpath("//dt")
    dd_elements = tree.xpath("//dd")

    for dt, dd in zip(dt_elements, dd_elements):
        key = dt.text_content().strip()

        # Behandlung von "Weiterführende Links"
        if key == "Weiterführende Links:":
            links = []
            for a in dd.xpath(".//a[@href]"):
                link_text = a.text_content().strip()
                link_url = a.get("href")
                if not link_url.startswith("http"):
                    link_url = "https://www.foerderdatenbank.de/" + link_url
                links.append({"text": link_text, "url": link_url})
            details[key] = links
        else:
            details[key] = dd.text_content().strip()
        
    # derive IDs based on URL 
    # parts of url that are unique - everything after "Foerderprogramm"
    url_parts = url.partition("Foerderprogramm/") # if this does not exist in the url, it'll give back "" which will result in an empty ID
    foerderprogramm_url_id = url_parts[2].replace("/", "-").replace(".html", "").lower()
    foerderprogramm_hash_id = hashlib.md5(foerderprogramm_url_id.encode()).hexdigest()

    return {"id_hash": foerderprogramm_hash_id, "id_url": foerderprogramm_url_id, "url": url, "title": title, "articles": articles, "details": details}
