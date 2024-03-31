import json
import os

# TODO: parse comp URL along with their names, and test it by turning off the competition check mechanism in comp callback function
# TODO: move the competition check mechanism from callback function to spider parses function


class BaseClass:
    def __init__(self):
        self.DATA_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../data")
        )
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.competitions = [
            "First Tier",
            "Domestic Cup",
            "Domestic Super Cup",
            "League Cup",
        ]

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
                all_country_code_parsed = all(
                    [
                        country in comps.keys()
                        and "country codes" in comps[country].keys()
                        for country in self.countries
                    ]
                )
        return all_country_code_parsed

    def write_to_json_file(self, json_content):
        """Writes to json in its own filepath (use case specific)

        Args:
            json_content (_type_): json content to write
        """
        super().write_to_json_file(self.FILE, json_content)

    def get_country_url(self, country_code) -> str:
        """Forms a country URL by appending country code

        Args:
            country_code (_type_): country code

        Returns:
            str: _description_
        """
        url: str = f"https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/{country_code}"
        return url

    def get_all_country_urls(self) -> dict:
        """returns a list of all countries URLs

        Returns:
            dict: {country: country_url}
        """
        with open(self.FILE, "r", encoding="utf-8") as file:
            comp_json = json.load(file)
            all_country_codes = {
                country: comp_json[country]["country_code"]
                for country in self.countries
            }
            all_country_urls = {
                country: self.get_country_url(code)
                for country, code in all_country_codes.items()
            }
        return all_country_urls


class CompetitionNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.FILE = os.path.join(self.DATA_DIR, "competitions.json")

    def domestic_comp_callback(self, response, country):
        """callback func that handles parsing of competition names if needed

        Args:
            response (_type_): spider response obj
            country (_type_): country for which comps are being parsed
        """
        have_all_domestic_comps: bool = self.have_all_domestic_comps()
        if not have_all_domestic_comps:
            comps = self.parse_domestic_comp_names(response, country)
            self.write_to_json_file(comps)

    def write_to_json_file(self, json_content):
        """Use case specific write to json file method

        Args:
            json_content (_type_): json to write

        """
        super().write_to_json_file(self.FILE, json_content)

    def parse_domestic_comp_names(self, response, country):
        """Parsed comp names for the country

        Args:
            response (_type_): spider response obj
            country (_type_): country name

        Returns:
            _type_: updated data containing comps info for a country to overwrite the file with
        """
        row_xpaths = self.get_domestic_comps_xpaths()
        rows = {
            country: {
                tier: response.xpath(row).get() for tier, row in row_xpaths.items()
            }
        }

        with open(self.FILE, "r", encoding="utf-8") as file:
            comps = json.load(file)
        comps[country]["competitions"] = rows[country]
        return comps

    def get_domestic_comps_xpaths(self) -> dict:
        """Get xpaths for table rows containing competition titles

        Returns:
            dict: {comp_tier:xpath to comp title}
        """
        xpaths = {}
        for comp in self.competitions:
            xpath_tier = '(//table[@class="items"])[1]/tbody/tr/td['
            xpath_tier += f'contains(text(), "{comp}") or '
            xpath_tier = xpath_tier[:-4] + "]/.."  # gives tr of comp hierarchy
            xpath_title = (
                xpath_tier
                + "//following-sibling::tr[1]/td[1]/table/tr/td[2]/a[1]/text()"
            )
            xpaths[comp] = xpath_title
        return xpaths

    def have_all_domestic_comps(self) -> bool:
        """Checks if all domestic comps are parsed in the file

        Returns:
            bool: True if parsed else False
        """
        # check if req file exists
        is_competitions_file: bool = os.path.isfile(self.FILE)
        # to check if all country codes are parsed
        all_domestic_comps_parsed: bool = is_competitions_file
        if is_competitions_file:
            with open(self.FILE, "r", encoding="utf-8") as file:
                comps = json.load(file)
                all_domestic_comps_parsed = all(
                    [
                        "competitions"
                        in comps[country].keys()  # competition info exists
                        and sorted(comps[country]["competitions"].keys())
                        == sorted(self.competitions)  # every competition info exists
                        for country in self.countries
                    ]
                )
        return all_domestic_comps_parsed
