import json
import os


class BaseClass:
    def __init__(self):
        self.DATA_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../data")
        )

    def write_to_json_file(self, file, json_content):
        """Writes json data on to json file

        Args:
            file (_type_): file path to write to
            json_content (_type_): json data to write on the file
        """
        with open(file, "w", encoding="utf-8") as f:
            json.dump(json_content, f, indent=4)


# class for dealing with country codes while obtaining data
class CountryCodes(BaseClass):
    def __init__(self):
        super().__init__()
        self.FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]

    def get_req_table_rows_xpath(self) -> str:
        """Gives xpath for our country rows in the table

        Args:
            countries (list): list of countries

        Returns:
            str: generated xpath
        """
        xpath = '//table[@class="items"]/tbody/tr/td[2]/img['
        for country in self.countries:
            xpath += f'contains(@alt, "{country}") or '
        xpath = xpath[:-4] + "]/../.."
        return xpath

    def parse_country_codes(self, response) -> str:
        """Parses country code from website into a json string

        Args:
            response (_type_): response obj of spider

        Returns:
            str: json string
        """
        xpath_rows = self.get_req_table_rows_xpath()
        # gets table rows from the URL
        table_rows = response.xpath(xpath_rows)
        # dict to store country-wise competition info
        competitions: dict = {}
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
            competitions[country] = {"country_code": country_code}
        # convert to json
        competitions = json.dumps(competitions)
        return competitions

    def have_all_country_codes(self) -> bool:
        """Checks whether all the county codes are parsed in the file

        Returns:
            bool: True if all codes are passed else False
        """
        # check if req file exists
        is_competitions_file: bool = os.path.isfile(self.FILE)
        # to check if all country codes are parsed
        all_country_code_parsed: bool = is_competitions_file
        if is_competitions_file:
            with open(self.FILE, "r", encoding="utf-8") as file:
                comps = json.load(file)
                all_country_code_parsed = False not in [
                    country in comps.keys() for country in self.countries
                ]
        return all_country_code_parsed

    def write_to_json_file(self, json_content):
        super().write_to_json_file(self.FILE, json_content)
