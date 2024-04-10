import scrapy

# from spider_utils.classes import CountryCodes, CompetitionNames, ClubNames
from .spider_utils.classes import CountryCodes, CompetitionNames, ClubNames


class BaseSpider(scrapy.Spider):
    name = "base"

    def __init__(self):
        super().__init__()
        self.country_codes = CountryCodes()
        self.comp_names = CompetitionNames()
        self.club_names = ClubNames()
