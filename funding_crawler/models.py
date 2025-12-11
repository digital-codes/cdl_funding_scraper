from pydantic import BaseModel, model_validator
from typing import List, Optional


class FundingProgramSchema(BaseModel):
    # this model represents a funding program (Förderprogramm). It includes loosely all information that can be found on a page of an individual funding program
    id_hash: str  # id for funding program - acquired by hashing the URL
    id_url: str  # id for funding program - taken from the url slug
    url: str  # the full url
    title: str  # title of the funding program
    description: Optional[str] = (
        None  # Tab "Kurzzusammenfassung" Missing for https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Land/Thueringen/schulbezogene-jahresprogramme-bne.html
    )
    more_info: Optional[str] = None  #  Tab "Zusatzinfos"
    legal_basis: Optional[str] = None  # Tab "Rechtsgrundlage"
    contact_info_institution: Optional[str] = (
        None  # contact info missing completely: https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMWi/dns-zukunftsfaehige-mobilitaet.html  # Ansprechpunkt
    )
    contact_info_street: Optional[str] = None  # Ansprechpunkt Street
    contact_info_city: Optional[str] = None  # Ansprechpunkt City
    contact_info_fax: Optional[str] = None  # Ansprechpunkt Fax
    contact_info_phone: Optional[str] = None  # Ansprechpunkt Phone
    contact_info_email: Optional[str] = None  # Ansprechpunkt Email
    contact_info_website: Optional[str] = None  # Ansprechpunkt Website
    funding_type: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/KfW/erp-foerderkredit-gruendung-und-nachfolge.html  # Förderart
    )
    funding_area: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/photonik-digitalisierte-automatisierte-produktion.html  # Förderbereich
    )
    funding_location: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/morphologische-entwi-klimawand-nord-ostsee-86933.html  # Fördergebiet
    )
    eligible_applicants: Optional[List[str]] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Bund/BMBF/transfer-inklusive-bildung.html  # Förderberechtigte
    )
    funding_body: Optional[str] = (
        None  # missing for e.g. https://www.foerderdatenbank.de/FDB/Content/DE/Foerderprogramm/Land/Rheinland-Pfalz/staerkung-forschung-tech-entwicklung-innovation.html  # Fördergeber "Bund" oder "Land"
    )
    further_links: Optional[List[str]] = None  # Weiterführende Links
    checksum: str  # used to see whether there were changes
    license_info: str  # fixed to CC-BY-ND 4.0.

    @model_validator(mode="after")
    def check_at_least_one_content_field(self) -> "FundingProgramSchema":
        """Ensure at least one of description, legal_basis, or more_info is provided."""
        if not any([self.description, self.legal_basis, self.more_info]):
            raise ValueError(
                "At least one of 'description', 'legal_basis', or 'more_info' must exist"
            )
        return self
