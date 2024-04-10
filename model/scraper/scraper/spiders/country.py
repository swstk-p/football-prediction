from .base import BaseSpider, scrapy


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

    def parse(self, response):
        """Parses the country codes and writes them

        Args:
            response (_type_): response object from spider
        """
        competitions = self.country_codes.parse_country_codes(response)
        self.country_codes.write_to_json_file(competitions)
