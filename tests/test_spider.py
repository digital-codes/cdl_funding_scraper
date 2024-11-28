from scrapy.http import HtmlResponse, Request
from src.spider import FundingSpider


def test_parse():
    spider = FundingSpider()

    with open("tests/overview.html") as f:
        html = f.read()

    response = HtmlResponse(url="http://example.com", body=html, encoding="utf-8")

    results = list(spider.parse(response))

    assert len(results) == 11

    assert (
        results[0].url
        == "https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Land/Sachsen/buergschaft-sachsen-beteiligung.html"
    )
    assert isinstance(results[0], Request)  # Ensure it's a Scrapy Request
    assert results[0].callback == spider.parse_details

    assert (
        results[1].url
        == "https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Land/NRW/garantien-von-beteiligungen-an-kmu-der-gewerbliche.html"
    )
    assert isinstance(results[1], Request)
    assert results[1].callback == spider.parse_details

    pagination_requests = [req for req in results if req.callback == spider.parse]

    assert len(pagination_requests) == 1, "No pagination request found"
    assert (
        pagination_requests[0].url
        == "https://www.foerderdatenbank.de/SiteGlobals/FDB/Forms/Suche/Foederprogrammsuche_Formular.html?gtp=%2526816beae2-d57e-4bdc-b55d-392bc1e17027_list%253D2&filterCategories=FundingProgram"
    )
    assert pagination_requests[0].callback == spider.parse


def test_parse_details_multi():
    spider = FundingSpider()

    # Mocked HTML response for the `parse_details` method
    with open("tests/detail_multi_desc.html") as f:
        detail_html = f.read()

    response = HtmlResponse(
        url="http://example.com/details/page1",
        body=detail_html,
        encoding="utf-8",
    )

    # Get generator output from parse_details
    result = list(spider.parse_details(response))

    # Assert the results
    assert len(result) == 1, "Should return exactly one item"
    item = result[0]

    # Check the extracted fields
    assert (
        item["title"]
        == "Beteiligungen der Mittelständischen Beteiligungsgesellschaft Thüringen mbH (MBG express)"
    )

    assert "description" in list(item["articles"].keys())
    assert "more_info" in list(item["articles"].keys())
    assert "legal_basis" in list(item["articles"].keys())

    assert item["details"]["funding_type"] == ["Beteiligung"]

    # Check the details section
    assert "contact_info" in item["details"]
    assert item["details"]["contact_info"]["email"] == "info@mbg-thueringen.de"

    # Check links
    assert "further_links" in list(item["details"].keys())
    links = item["details"]["further_links"]

    assert len(links) == 1

    assert links[0]["text"] == "MBG express"
    assert links[0]["url"] == "https://mbg-thueringen.de/loesungen/mbg-express/"


def test_parse_details_single():
    spider = FundingSpider()

    # Mocked HTML response for the `parse_details` method
    with open("tests/detail_single_desc.html") as f:
        detail_html = f.read()

    response = HtmlResponse(
        url="http://example.com/details/page1",
        body=detail_html,
        encoding="utf-8",
    )

    # Get generator output from parse_details
    result = list(spider.parse_details(response))

    # Assert the results
    assert len(result) == 1, "Should return exactly one item"
    item = result[0]

    assert len(item["details"]["funding_area"]) == 4

    # Check the extracted fields
    assert (
        item["title"]
        == "Förderung von Unternehmerinnen in der Vorgründungsphase (EXIST-Women) im Rahmen des Förderprogramms „Existenzgründungen aus der Wissenschaft“"
    )

    assert "description" in list(item["articles"].keys())

    # Check the details section
    assert "contact_info" in item["details"]
    assert item["details"]["contact_info"]["email"] == "ptj-exist-women@fz-juelich.de"
    assert (
        item["details"]["contact_info"]["institution"] == "Projektträger Jülich (PtJ)"
    )

    # Check links
    assert "further_links" in list(item["details"].keys())
    links = item["details"]["further_links"]

    assert len(links) == 4

    assert links[0]["text"] == "EXIST-Women"
    assert (
        links[0]["url"]
        == "https://www.exist.de/EXIST/Navigation/DE/Gruendungsfoerderung/EXIST-WOMEN/EXIST-WOMEN/exist-women.html"
    )
