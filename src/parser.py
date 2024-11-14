# src/parser.py
import lxml.html


def parse_program_page(html_bytes: bytes, url: str) -> dict:
    """
    Parst die Seite und extrahiert die benötigten Informationen.
    Fügt auch die Quell-URL zu den extrahierten Daten hinzu.

    :param html_bytes: bytes-Repräsentation des HTMLs
    :param url: Die URL der Seite, die geparst wird
    :return: Ein Dictionary mit den extrahierten Informationen und der Quell-URL
    """
    tree = lxml.html.fromstring(html_bytes)

    title = tree.xpath("//h1[@class='title']")[0].text_content().strip()

    articles = {}
    article_nodes = tree.xpath("//div[@class='content']//article")
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

    return {"url": url, "title": title, "articles": articles, "details": details}
