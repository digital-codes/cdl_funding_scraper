from funding_crawler.helpers import get_hits_count


def test_get_hits_count():
    url = "https://www.foerderdatenbank.de/SiteGlobals/FDB/Forms/Suche/Foederprogrammsuche_Formular.html?resourceId=0065e6ec-5c0a-4678-b503-b7e7ec435dfd&input_=23adddb0-dcf7-4e32-96f5-93aec5db2716&pageLocale=de&filterCategories=FundingProgram"
    hits_count = get_hits_count(url)
    print(f"Hits count: {hits_count}")
