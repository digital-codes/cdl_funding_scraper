from pydantic import BaseModel
from typing import List, Optional


class FundingProgramSchema(BaseModel):
    id_hash: str
    id_url: str
    url: str
    title: str

    description: str
    more_info: Optional[str] = None
    legal_basis: Optional[str] = None

    contact_info_institution: str
    contact_info_street: Optional[str] = None
    contact_info_city: Optional[str] = None
    contact_info_fax: Optional[str] = None
    contact_info_phone: Optional[str] = None
    contact_info_email: Optional[str] = None
    contact_info_website: Optional[str] = None

    funding_type: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/KfW/erp-foerderkredit-gruendung-und-nachfolge.html
    )
    funding_area: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/photonik-digitalisierte-automatisierte-produktion.html
    )
    funding_location: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/morphologische-entwi-klimawand-nord-ostsee-86933.html
    )
    eligible_applicants: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/transfer-inklusive-bildung.html
    )
    funding_body: Optional[str] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Land/Rheinland-Pfalz/staerkung-forschung-tech-entwicklung-innovation.html
    )

    further_links: Optional[List[str]] = None
