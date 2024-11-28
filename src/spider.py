import logging
from src.settings import BASE_URL, LOG_FILE

# MAX_RESULTS_PAGES, SLEEP_INTERVAL
import hashlib

from scrapy import Request
from scrapy import Spider

# Logging konfigurieren
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
)

translate_map = {
    "Kurzzusammenfassung": "description",
    "Zusatzinfos": "more_info",
    "Rechtsgrundlage": "legal_basis",
    "Ansprechpunkt": "contact_info",
    "Weiterführende Links": "further_links",
    "Förderart": "funding_type",
    "Förderbereich": "funding_area",
    "Fördergebiet": "funding_location",
    "Förderberechtigte": "eligible_applicants",
    "Fördergeber": "funding_body",
}


class FundingSpider(Spider):
    name = "funding"
    start_urls = [BASE_URL]

    def parse(self, response):
        urls = response.css(
            'div.card--fundingprogram > p.card--title > a::attr("href")'
        ).extract()

        for url in urls:
            url = response.urljoin(url)
            yield Request(url=url, callback=self.parse_details)

        next_page = response.css(
            'div.container.content--main.content--search.for-aside-left div.content div.pagination ul li a.forward.button::attr("href")'
        ).get()

        if next_page is not None:
            yield response.follow(next_page, self.parse)

    def parse_details(self, response):
        # Extract the title
        title = response.xpath("//h1[@class='title']/text()").get().strip()

        # Extract articles
        articles = {}

        tab_names = response.xpath(
            "/html/body/main/div[2]/div/div[1]/h2/span//text()"
        ).getall()

        if tab_names:
            article_nodes = response.xpath("//div[@class='content']//article")

            for i, article in enumerate(article_nodes):
                content = article.get()
                articles[translate_map[tab_names[i].strip()]] = content
        else:
            # Workaround for pages without tabs
            content_node = response.xpath(
                "//main/div[@class='jumbotron']/following-sibling::div/div[@class='content']"
            ).get()
            articles = {"description": content_node}

        # Extract details
        details = {}
        dt_elements = response.xpath("//dt")
        dd_elements = response.xpath("//dd")
        for dt, dd in zip(dt_elements, dd_elements):
            key = dt.xpath("text()").get().strip().replace(":", "")
            key = translate_map[key]

            if key in [
                "funding_type",
                "funding_area",
                "funding_location",
                "eligible_applicants",
                "funding_body",
            ]:
                lst_str = dd.xpath("text()").get().strip()
                details[key] = lst_str.split(", ")

            elif key == "further_links":
                links = []
                for link in dd.xpath(".//a[@href]"):
                    link_text = link.xpath(".//span/text()").get().strip()
                    link_url = link.xpath("@href").get()

                    if not link_url.startswith("http"):
                        link_url = "https://www.foerderdatenbank.de/" + link_url
                    links.append({"text": link_text, "url": link_url})
                details[key] = links

            elif key == "contact_info":
                contact_info = {}

                contact_info["institution"] = " ".join(
                    dd.xpath(
                        ".//a[@title='Öffnet die Einzelsicht']/span[@class='link--label']//text()"
                    ).getall()
                ).strip()

                contact_info["street"] = (
                    dd.xpath(".//p[@class='adr']/text()").get().strip()
                )
                contact_info["city"] = (
                    dd.xpath(".//p[@class='locality']/text()").get().strip()
                )

                contact_info["fax"] = (
                    dd.xpath(".//p[@class='fax']/text()").re_first(r"Fax:\s*(.*)") or ""
                ).strip()

                contact_info["phone"] = (
                    dd.xpath(".//p[@class='tel']/text()").re_first(r"Tel:\s*(.*)") or ""
                ).strip()

                contact_info["email"] = (
                    dd.xpath(".//p[@class='email']/a[@href]")
                    .xpath("@href")
                    .re_first(r"mailto:(.*)")
                ).strip()

                contact_info["website"] = (
                    dd.xpath(".//p[@class='website']/a[@href]").xpath("@href").get()
                ).strip()

                details[key] = contact_info
            else:
                details[key] = dd.xpath("text()").get().strip()

        # Generate IDs based on URL
        url_parts = response.url.partition("Foerderprogramm/")
        if url_parts[1] == "":
            url_parts = response.url.partition("Archiv/")
        foerderprogramm_url_id = (
            url_parts[2].replace("/", "-").replace(".html", "").lower()
        )
        foerderprogramm_hash_id = hashlib.md5(
            foerderprogramm_url_id.encode()
        ).hexdigest()

        # Yield the extracted data
        yield {
            "id_hash": foerderprogramm_hash_id,
            "id_url": foerderprogramm_url_id,
            "url": response.url,
            "title": title,
            "articles": articles,
            "details": details,
        }
