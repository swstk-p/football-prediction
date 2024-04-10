import scrapy

# from spider_utils.classes import CountryCodes, CompetitionNames, ClubNames
from .spider_utils.classes import CountryCodes, CompetitionNames, ClubNames


class CountryCodeSpider(scrapy.Spider):
    name = "country_code"

    def __init__(self):
        super().__init__()
        self.country_codes = CountryCodes()

    def start_requests(self):
        """Checks if all the country codes are already available and sends a request if they aren't

        Yields:
            _type_: scrapy.Request
        """
        have_all_country_codes: bool = self.country_codes.have_all_country_codes()
        if not have_all_country_codes:
            url = "https://www.transfermarkt.com/wettbewerbe/europa"
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """Parses the country codes and writes them

        Args:
            response (_type_): response object from spider
        """
        competitions = self.country_codes.parse_country_codes(response)
        self.country_codes.write_to_json_file(competitions)


class CompNameSpider(scrapy.Spider):
    name = "comp_name"

    def __init__(self):
        super().__init__()
        self.country_codes = CountryCodes()
        self.comp_names = CompetitionNames()

    def start_requests(self):
        """Checks if all the competitions are available; if not then generates urls from country codes and sends requests

        Yields:
            _type_: scrapy.Request
        """
        have_all_domestic_comps: bool = self.comp_names.have_all_domestic_comps()
        if not have_all_domestic_comps:
            # get urls for each country pages
            all_country_urls = self.country_codes.get_all_country_urls()
            # follow each country urls
            for country, url in all_country_urls.items():
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_domestic_comp,
                    cb_kwargs={"country": country},
                )
        # TODO: scrape European competitions as well

    def parse_domestic_comp(self, response, country):
        """Parses the competition names from the response and writes them

        Args:
            response (_type_): response object from spider
            country (_type_): country name
        """
        comps = self.comp_names.parse_domestic_comp_names(response, country)
        self.comp_names.write_to_json_file(comps)


class ClubNameSpider(scrapy.Spider):
    name = "club_name"

    def __init__(self):
        super().__init__()
        self.country_codes = CountryCodes()
        self.comp_names = CompetitionNames()
        self.club_names = ClubNames()

    def start_requests(self):
        """Checks if teams for all the leagues in all seasons are available; if not then generates url for each league each season and sends request

        Yields:
            _type_: scrapy.Request
        """
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

    def parse(self, response, league, season):
        """Parses the club names from the response and writes them

        Args:
            response (_type_): response object from spider
            league (_type_): league name
            season (_type_): season
        """
        clubs = self.club_names.parse_club_names(response, league, season)
        self.club_names.write_to_json_file(clubs)
