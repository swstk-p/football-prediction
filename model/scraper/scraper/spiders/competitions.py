import scrapy
import json
import os

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data"))
COMPETITION_FILE = os.path.join(DATA_DIR, "competitions.json")


class CompetitionSpider(scrapy.Spider):
    name = "competitions"

    def __init__(self):
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.start_urls = ["https://www.transfermarkt.com/wettbewerbe/europa"]

    # initial parse function, gets country code
    def parse(self, response):
        xpath_rows = self.get_req_table_rows_xpath(self.countries)
        # gets table rows from the URL
        table_rows = response.xpath(xpath_rows)
        # dict to store country-wise competition info
        competitions: list[dict] = []
        for row in table_rows:
            # country name
            country = row.xpath("td[2]/img/@alt").getall()[0]
            # country code for URL to further parse
            country_code = (
                row.xpath("td[2]/img/@src")
                .getall()[0]
                .split("/")[-1]
                .split("?")[0]
                .split(".")[0]
            )
            # write code info to dict
            competitions.append({country: {"country_code": country_code}})
        # convert to json
        competitions = json.dumps(competitions)

        self.write_to_json_file(COMPETITION_FILE, competitions)

    def write_to_json_file(self, file, json_content):
        """Writes json data on to json file

        Args:
            file (_type_): file path to write to
            json_content (_type_): json data to write on the file
        """
        with open(file, "w", encoding="utf-8") as f:
            json.dump(json_content, f, indent=4)

    def get_req_table_rows_xpath(self, countries: list) -> str:
        """Gives xpath for our country rows in the table

        Args:
            countries (list): list of countries

        Returns:
            str: generated xpath
        """
        xpath = '//table[@class="items"]/tbody/tr/td[2]/img['
        for country in countries:
            xpath += f'contains(@alt, "{country}") or '
        xpath = xpath[:-4] + "]/../.."
        return xpath
