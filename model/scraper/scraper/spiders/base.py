import scrapy

# from spider_utils.classes import CountryCodes, CompetitionNames, ClubNames
from .spider_utils.classes import (
    CountryCodes,
    CompetitionNames,
    ClubNames,
    Fixtures,
    Injuries,
)


class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self):
        super().__init__()

    def get_country_code_obj(self) -> CountryCodes:
        """Returns an object to deal with parsing country codes.

        Returns:
            CountryCodes: an object of class CountryCodes
        """
        return CountryCodes()

    def get_comp_name_obj(self) -> CompetitionNames:
        """Returns an object to deal with parsing competition names.

        Returns:
            CompetitionNames: an object of class CompetitionNames
        """
        return CompetitionNames()

    def get_club_name_obj(self) -> ClubNames:
        """Returns an object to deal with parsing club names.

        Returns:
            ClubNames: an object of class ClubNames
        """
        return ClubNames()

    def get_fixture_obj(self) -> Fixtures:
        """Returns an object to deal with parsing fixtures.

        Returns:
            Fixtures: an object of class Fixtures
        """
        return Fixtures()

    def get_injury_obj(self) -> Injuries:
        """Returns an object to deal with parsing injuries.

        Returns:
            Injuries: an object of class Injuries
        """
        return Injuries()
