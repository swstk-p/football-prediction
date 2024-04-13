from .base import BaseSpider, scrapy
from pprint import pp


class CountryCodeSpider(BaseSpider):
    name = "country_code"

    def __init__(self):
        super().__init__()

    def start_requests(self):
        """Checks if all the country codes are already available and sends a request if they aren't

        Yields:
            _type_: scrapy.Request
        """
        have_all_country_codes: bool = self.country_codes.have_all_country_codes()
        if not have_all_country_codes:
            url = "https://www.transfermarkt.com/wettbewerbe/europa"
            yield scrapy.Request(url=url, callback=self.parse)
        else:
            print("*********************************************************")
            print("NOT SCRAPED BECAUSE ALL COUNTRY CODES PRESENT IN DATABASE")
            print("*********************************************************")

    def parse(self, response):
        """Parses the country codes and records them

        Args:
            response (_type_): response object from spider
        """
        competitions = self.country_codes.parse_country_codes(response)
        self.country_codes.record_in_db(competitions)
