import scrapy
import json
import os
from ..spider_utils.classes import CountryCodes

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data"))
COMPETITION_FILE = os.path.join(DATA_DIR, "competitions.json")


class CompetitionSpider(scrapy.Spider):
    name = "competitions"

    def __init__(self):
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.start_urls = ["https://www.transfermarkt.com/wettbewerbe/europa"]
        self.country_codes = CountryCodes()

    def parse(self, response):
        have_all_country_codes: bool = self.country_codes.have_all_country_codes()
        if not have_all_country_codes:
            competitions = self.country_codes.parse_country_codes(response)
            self.country_codes.write_to_json_file(competitions)
