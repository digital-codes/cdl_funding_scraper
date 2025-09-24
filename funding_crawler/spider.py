import hashlib
from scrapy import Request, Spider
from datetime import datetime
from funding_crawler.helpers import compute_checksum, gen_license
from w3lib.url import canonicalize_url

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
    """
    A Scrapy spider for extracting funding program details from a website.
    The spider crawls through pages of funding programs, extracts program details,
    and navigates through pagination.
    """

    name = "funding"

    def __init__(self, *args, **kwargs):
        super(FundingSpider, self).__init__(*args, **kwargs)
        self.total_cards_found = 0
        self.unique_urls = {}  # URL -> (page_number, page_url) mapping
        self.page_count = 0

    def parse(self, response):
        """
        Parse the response from the main page listing funding programs.

        Extracts URLs for individual funding program details and follows them
        for further parsing. Also handles pagination to fetch additional program listings.

        Args:
            response (Response): The HTTP response object for the current page.

        Yields:
            Request: Requests for detailed program pages and the next pagination page.
        """
        urls = response.css(
            'div.card--fundingprogram > p.card--title > a::attr("href")'
        ).extract()

        cards = response.css("div.card--fundingprogram")
        self.total_cards_found += len(cards)
        self.page_count += 1

        self.logger.debug(
            f"Processing page {self.page_count}: {response.url} (found {len(cards)} cards)"
        )

        if not cards:
            self.logger.warning(
                f"No funding program cards found on page: {response.url}"
            )
        elif len(cards) < 10 or len(cards) > 10:
            self.logger.warning(
                f"Found {len(cards)} funding program cards on page: {response.url} (expected 10)"
            )
        else:
            pass

        if self.page_count % 10 == 0:
            self.logger.info(f"Total cards found so far: {self.total_cards_found}")

        for i, card in enumerate(cards):
            title_link = card.css('p.card--title > a::attr("href")').get()
            title_text = card.css("p.card--title > a::text").get()

            if not title_link:
                self.logger.warning(f"Card {i+1} on {response.url} missing title link")
            if not title_text:
                self.logger.warning(f"Card {i+1} on {response.url} missing title text")

        for url in urls:
            full_url = response.urljoin(url)
            normalized = canonicalize_url(full_url)

            if normalized in self.unique_urls:
                original_page_num, original_page_url = self.unique_urls[normalized]
                self.logger.warning(
                    f"Skipping duplicate URL: {normalized}. Duplicate found on page {self.page_count}: {response.url}. Originally found on page {original_page_num}: {original_page_url}"
                )
                continue

            self.unique_urls[normalized] = (self.page_count, response.url)
            yield Request(url=normalized, callback=self.parse_details)

        if self.page_count % 10 == 0:
            self.logger.info(f"Total unique URLs found so far: {len(self.unique_urls)}")

        next_page = response.css('a.forward.button::attr("href")').get()

        if next_page is not None and next_page != "":
            yield response.follow(next_page, self.parse)

    def parse_details(self, response):
        """
        Parse the response from a funding program detail page.

        Extracts details such as title, description, funding information, contact details,
        and additional links. Computes unique identifiers and a checksum for the data.

        Args:
            response (Response): The HTTP response object for the detail page.

        Yields:
            dict: A dictionary containing the extracted program details.
        """
        dct = {}

        dct["title"] = "".join(
            response.xpath("//h1[@class='title']//text()").getall()
        ).strip()
        dct["title"] = dct["title"] if dct["title"] else None

        if not dct["title"]:
            self.logger.warning(f"No title found on page: {response.url}")
            return  # Don't yield items without titles

        tab_names = response.xpath(
            "/html/body/main/div[2]/div/div[1]/h2/span//text()"
        ).getall()

        if tab_names:
            article_nodes = response.xpath("//div[@class='content']//article")

            for i, article in enumerate(article_nodes):
                content = article.get()
                key = translate_map.get(tab_names[i].strip())
                dct[key] = content
        else:
            content_node = response.xpath(
                "//main/div[@class='jumbotron']/following-sibling::div/div[@class='content']"
            ).get()
            dct["description"] = content_node

            if not content_node:
                self.logger.warning(
                    f"No content structure found on page: {response.url}"
                )

        dt_elements = response.xpath("//dt")
        dd_elements = response.xpath("//dd")

        if (
            not dt_elements
            and not dd_elements
            and not tab_names
            and not dct.get("description")
        ):
            self.logger.warning(
                f"No extractable data fields found on page: {response.url}"
            )

        for dt, dd in zip(dt_elements, dd_elements):
            key = dt.xpath("text()").get()
            if key:
                key = translate_map.get(key.strip().replace(":", ""))
                if not key:
                    continue
            else:
                continue

            if key in [
                "funding_type",
                "funding_area",
                "funding_location",
                "eligible_applicants",
            ]:
                lst_str = dd.xpath("text()").get()
                lst = lst_str.strip().split(", ") if lst_str else []
                dct[key] = lst if lst else None

            elif key == "funding_body":
                # /html/body/main/div[1]/div[2]/div/dl/dd[4]/p/a/span
                str = dd.xpath(
                    "p[@class='card--title']/a[@title='Öffnet die Einzelsicht']/span[@class='link--label']/text()"
                ).get()
                dct[key] = str.strip() if str else None

            elif key == "further_links":
                links = []
                for link in dd.xpath(".//a[@href]"):
                    link_url = link.xpath("@href").get()
                    if not link_url.startswith("http"):
                        link_url = "https://www.foerderdatenbank.de/" + link_url
                    links.append(link_url)

                dct[key] = links if links else None

            elif key == "contact_info":
                dct["contact_info_institution"] = (
                    " ".join(
                        dd.xpath(
                            ".//a[@title='Öffnet die Einzelsicht']/span[@class='link--label']//text()"
                        ).getall()
                    ).strip()
                    or None
                )

                dct["contact_info_street"] = (
                    dd.xpath(".//p[@class='adr']/text()").get() or ""
                ).strip() or None

                dct["contact_info_city"] = (
                    dd.xpath(".//p[@class='locality']/text()").get() or ""
                ).strip() or None

                dct["contact_info_fax"] = (
                    dd.xpath(".//p[@class='fax']/text()").re_first(r"Fax:\s*(.*)")
                    or None
                )

                dct["contact_info_phone"] = (
                    dd.xpath(".//p[@class='tel']/text()").re_first(r"Tel:\s*(.*)")
                    or None
                )

                dct["contact_info_email"] = (
                    dd.xpath(".//p[@class='email']/a[@href]")
                    .xpath("@href")
                    .re_first(r"mailto:(.*)")
                    or None
                )

                dct["contact_info_website"] = (
                    dd.xpath(".//p[@class='website']/a[@href]").xpath("@href").get()
                    or None
                )

            else:
                value = dd.xpath("text()").get()
                dct[key] = value.strip() if value else None

        url_parts = response.url.partition("Foerderprogramm/")

        if url_parts[1] == "":
            url_parts = response.url.partition(
                "Archiv/"
            )  # e.g. 'https://www.foerderdatenbank.de/FDB/Content/DE/Archiv/innovativer-schiffbau-sichert-arbeitsplaetze.html'

        foerderprogramm_url_id = (
            url_parts[2].replace("/", "-").replace(".html", "").lower()
        )
        foerderprogramm_hash_id = hashlib.md5(
            foerderprogramm_url_id.encode()
        ).hexdigest()

        dct["url"] = response.url
        dct["id_hash"] = foerderprogramm_hash_id
        dct["id_url"] = foerderprogramm_url_id

        ignore_fields = ["url", "id_hash", "id_url"]
        watch_fields = [x for x in list(dct.keys()) if x not in ignore_fields]

        dct["checksum"] = compute_checksum(dct, watch_fields)

        date = datetime.today()
        dct["license_info"] = gen_license(dct["title"], date, dct["url"])

        yield dct
