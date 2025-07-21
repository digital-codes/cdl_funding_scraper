from scrapy.http import HtmlResponse, Request
from funding_crawler.spider import FundingSpider
from pydantic import ValidationError
from funding_crawler.models import FundingProgramSchema
import requests


def test_parse():
    spider = FundingSpider()

    with open("tests/test_scrapy/overview.html") as f:
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

    with open("tests/test_scrapy/detail_multi_desc.html") as f:
        detail_html = f.read()

    response = HtmlResponse(
        url="http://example.com/details/page1",
        body=detail_html,
        encoding="utf-8",
    )

    results = list(spider.parse_details(response))

    assert len(results) == 1, "Should return exactly one item"

    item = results[0]

    try:
        validated_item = FundingProgramSchema(**item)
    except ValidationError as e:
        raise AssertionError(f"Validation error: {e}")

    assert validated_item.title == (
        "Beteiligungen der Mittelständischen Beteiligungsgesellschaft Thüringen mbH (MBG express)"
    )
    assert validated_item.contact_info_email == "info@mbg-thueringen.de"
    assert validated_item.funding_type == ["Beteiligung"]
    assert len(validated_item.further_links) == 1
    assert (
        validated_item.further_links[0]
        == "https://mbg-thueringen.de/loesungen/mbg-express/"
    )
    assert validated_item.funding_body is None


def test_parse_details_single():
    spider = FundingSpider()

    with open("tests/test_scrapy/detail_single_desc.html") as f:
        detail_html = f.read()

    response = HtmlResponse(
        url="http://example.com/details/page1",
        body=detail_html,
        encoding="utf-8",
    )

    results = list(spider.parse_details(response))

    assert len(results) == 1, "Should return exactly one item"

    item = results[0]

    try:
        validated_item = FundingProgramSchema(**item)
    except ValidationError as e:
        raise AssertionError(f"Validation error: {e}")

    assert validated_item.title == (
        "Förderung von Unternehmerinnen in der Vorgründungsphase (EXIST-Women) "
        "im Rahmen des Förderprogramms „Existenzgründungen aus der Wissenschaft“"
    )
    assert validated_item.contact_info_email == "ptj-exist-women@fz-juelich.de"
    assert validated_item.contact_info_institution == "Projektträger Jülich (PtJ)"
    assert len(validated_item.funding_area) == 4
    assert len(validated_item.further_links) == 4
    assert validated_item.further_links[0] == (
        "https://www.exist.de/EXIST/Navigation/DE/Gruendungsfoerderung/EXIST-WOMEN/"
        "EXIST-WOMEN/exist-women.html"
    )
    assert (
        validated_item.funding_body
        == "Bundesministerium für Wirtschaft und Klimaschutz (BMWK)"
    )


def test_parse_details_single_alt():
    spider = FundingSpider()

    with open("tests/test_scrapy/details_single_desc_alt.html") as f:
        detail_html = f.read()

    response = HtmlResponse(
        url="http://example.com/details/page1",
        body=detail_html,
        encoding="utf-8",
    )

    results = list(spider.parse_details(response))

    assert len(results) == 1, "Should return exactly one item"

    item = results[0]

    try:
        validated_item = FundingProgramSchema(**item)
    except ValidationError as e:
        raise AssertionError(f"Validation error: {e}")

    assert (
        validated_item.funding_body
        == "Bundesministerium für Bildung und Forschung (BMBF)"
    )


def test_parse_details_single_fail_07_25():
    spider = FundingSpider()

    response = requests.get(
        "https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BKM/deutsch-franzoesische-kooperationen-film-ffa.html"
    )

    detail_html = HtmlResponse(
        url="https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BKM/deutsch-franzoesische-kooperationen-film-ffa.html",
        body=response.content,
        encoding="utf-8",
    )

    results = list(spider.parse_details(detail_html))

    assert len(results) == 1, "Should return exactly one item"

    item = results[0]

    try:
        validated_item = FundingProgramSchema(**item)
    except ValidationError as e:
        raise AssertionError(f"Validation error: {e}")

    print(validated_item.checksum)
