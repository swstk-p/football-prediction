import scrapy
import json
import os
from ..spider_utils.classes import CountryCodes, CompetitionNames, ClubNames

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data"))
COMPETITION_FILE = os.path.join(DATA_DIR, "competitions.json")


class CompetitionSpider(scrapy.Spider):
    name = "transfermarkt"

    def __init__(self):
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.start_urls = ["https://www.transfermarkt.com/wettbewerbe/europa"]
        self.country_codes = CountryCodes()
        self.comp_names = CompetitionNames()
        self.club_names = ClubNames()

    def parse(self, response):
        # parse country codes if not available yet
        have_all_country_codes: bool = self.country_codes.have_all_country_codes()
        if not have_all_country_codes:
            competitions = self.country_codes.parse_country_codes(response)
            self.country_codes.write_to_json_file(competitions)
        # parse comp names and urls if not available yet
        have_all_domestic_comps: bool = self.comp_names.have_all_domestic_comps()
        if not have_all_domestic_comps:
            # get urls for each country pages
            all_country_urls = self.country_codes.get_all_country_urls()
            # follow each country urls
            for country, url in all_country_urls.items():
                yield scrapy.Request(
                    url=url,
                    callback=self.comp_names.domestic_comp_callback,
                    cb_kwargs={"country": country},
                )
        # TODO: scrape European competitions as well
        # parse team names and urls if not available
        have_all_club_names = self.club_names.have_all_leagues_seasons_club_names()
        if not have_all_club_names:
            all_clubs_urls_info = self.club_names.get_all_seasons_leagues_url()
            for url_info in all_clubs_urls_info:
                yield scrapy.Request(
                    url=url_info["url"],
                    callback=self.club_names.all_club_names_callback,
                    cb_kwargs={
                        "league": url_info["league"],
                        "season": url_info["season"],
                    },
                )
