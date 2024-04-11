import json
import os

# TODO: write data onto db instead of json


class BaseClass:
    def __init__(self):
        self.DATA_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../../data")
        )
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.competitions = [
            "First Tier",
            "Domestic Cup",
            "Domestic Super Cup",
            "League Cup",
        ]
        # TODO: make seasons have all data of current season + past five seasons instead of hardcoding it
        self.seasons = ["2018", "2019", "2020", "2021", "2022", "2023"]

    def write_to_json_file(self, file, json_content):
        """Writes json data on to json file

        Args:
            file (_type_): file path to write to
            json_content (_type_): json data to write on the file
        """
        with open(file, "w", encoding="utf-8") as f:
            json.dump(json_content, f, indent=4)

    def is_file_empty(self, file) -> bool:
        """determines if the file is empty

        Args:
            file (_type_): file path

        Returns:
            bool: true if it's empty or doesn't exist, else false
        """
        if not os.path.isfile(file):
            return True
        else:
            return os.path.getsize(file) <= 0


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
                        and "country_code" in comps[country].keys()
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


# class for dealing with competition names while obtaining data
class CompetitionNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.intl_comps = {
            "First Tier": "UEFA Champions League",
            "Second Tier": "UEFA Europa League",
            "Third Tier": "UEFA Europa Conference League",
            "Cup": "UEFA Super Cup",
        }

    def domestic_comp_callback(self, response, country):
        """callback func that handles parsing of competition names if needed

        Args:
            response (_type_): spider response obj
            country (_type_): country for which comps are being parsed
        """
        comps = self.parse_domestic_comp_names(response, country)
        self.write_to_json_file(comps)

    def write_to_json_file(self, json_content):
        """Use case specific write to json file method

        Args:
            json_content (_type_): json to write

        """
        super().write_to_json_file(self.FILE, json_content)

    def parse_domestic_comp_names(self, response, country):
        """Parse comp names and urls for the country

        Args:
            response (_type_): spider response obj
            country (_type_): country name

        Returns:
            _type_: updated data containing comps info for a country to overwrite the file with
        """
        row_xpaths = (
            self.get_domestic_comps_xpaths()
        )  # row_xpaths = {tier: [title, url]}
        rows = {
            country: {
                tier: (
                    response.xpath(row[0]).get(),
                    response.xpath(row[1]).get(),
                )
                for tier, row in row_xpaths.items()
            }
        }
        with open(self.FILE, "r", encoding="utf-8") as file:
            comps = json.load(
                file
            )  # retaining previous info (we are sure the file exists because of country codes)
        comps[country]["competitions"] = rows[country]  # adding to retained info
        return comps

    def parse_intl_comp_names(self, response):
        """Parse comp names and urls for UEFA competitions

        Args:
            response (_type_): response object from spider

        Returns:
            _type_: updated data containing comps info to overwrite the file with
        """
        row_xpaths = self.get_intl_comps_xpath()
        rows = {
            "European": {
                "competitions": {
                    tier: (response.xpath(row[0]).get(), response.xpath(row[1]).get())
                    for tier, row in row_xpaths.items()
                }
            }
        }
        with open(self.FILE, "r", encoding="utf-8") as file:
            comps = json.load(file)
        comps["European"] = rows["European"]
        return comps

    def get_intl_comps_xpath(self) -> dict:
        """Get xpaths for elements containing UEFA competition titles and urls

        Returns:
            dict: {comp_tier: (xpath to comp title, xpath to comp url)}
        """
        xpaths = {}
        for tier, name in self.intl_comps.items():
            xpath_name = f'//div[@class="large-4 columns"]/div[@class="box"]/div[contains(text(), "Cups")]/../a[./@title="{name}"]/@title'
            xpath_url = f'//div[@class="large-4 columns"]/div[@class="box"]/div[contains(text(), "Cups")]/../a[./@title="{name}"]/@href'
            xpaths[tier] = (xpath_name, xpath_url)
        return xpaths

    def get_domestic_comps_xpaths(self) -> dict:
        """Get xpaths for table rows containing competition titles and urls

        Returns:
            dict: {comp_tier:[xpath to comp title, xpath to comp url]}
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
            xpath_url = (
                xpath_tier
                + "//following-sibling::tr[1]/td[1]/table/tr/td[2]/a[1]/@href"
            )
            xpaths[comp] = (xpath_title, xpath_url)
        return xpaths

    def have_all_domestic_comps(self) -> bool:
        """Checks if all domestic comps are parsed in the file

        Returns:
            bool: True if parsed else False
        """
        # check if req file exists
        is_competitions_file: bool = os.path.isfile(self.FILE)
        # to check if all comp names are parsed
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

    def have_all_intl_comps(self) -> bool:
        """Checks if all intl (UEFA) comps are parsed in the file

        Returns:
            bool: True if parsed else False
        """
        is_competitions_file: bool = os.path.isfile(self.FILE)
        # to check if all comp names are parsed
        all_intl_comps_parsed: bool = is_competitions_file
        if is_competitions_file:
            with open(self.FILE, "r", encoding="utf-8") as file:
                comps = json.load(file)
                all_intl_comps_parsed = all(
                    [
                        "European" in comps.keys()
                        and sorted(comps["European"]["competitions"].keys())
                        == sorted(self.intl_comps.keys())
                    ]
                )
        return all_intl_comps_parsed


# class to deal to all the clubs names in all leagues and seasons
class ClubNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.COMP_FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.CLUB_FILE = os.path.join(self.DATA_DIR, "clubs.json")

    def get_comp_url(self, country: str, comp: str, season: str) -> dict:
        """Provides the competition url based on country and competition

        Args:
            country (str): Country name
            comp (str): Competition Type

        Returns:
            str: {url:url of that competition, league:league name, season:season start year}
        """
        with open(self.COMP_FILE, "r+", encoding="utf-8") as file:
            comps_info = json.load(file)
            url: str = (
                "https://www.transfermarkt.com"
                + comps_info[country]["competitions"][comp][1]
                + "/plus/?saison_id="
                + season
            )
            url_info = {
                "url": url,
                "league": comps_info[country]["competitions"][comp][0],
                "season": season,
            }
        return url_info

    def get_all_seasons_leagues_url(self) -> list[str]:
        """Provides league urls for all concerned countries

        Returns:
            list[str]: list of urls
        """
        league_urls = [
            self.get_comp_url(country, "First Tier", season)
            for country in self.countries
            for season in self.seasons
        ]
        return league_urls

    def all_club_names_callback(self, response, league, season):
        """callback for league page URL request to parse all club names and urls

        Args:
            response (_type_): responses obj from spider
            league (_type_): league name
            season (_type_): season start year
        """
        clubs = self.parse_club_names(response, league, season)
        self.write_to_json_file(clubs)

    def parse_club_names(self, response, league, season) -> dict:
        """Parses club names, urls and returns them

        Args:
            response (_type_): response obj from spider
            league (_type_): name of league
            season (_type_): start year of season

        Returns:
            dict: {league:{season:[(club name, club page url)]}}
        """
        all_clubs_rows_xpath = self.get_all_clubs_row_xpath()
        rows = response.xpath(all_clubs_rows_xpath)
        rows = {
            league: {
                season: [
                    (row.xpath("text()").get(), row.xpath("@href[1]").get())
                    for row in rows
                ]
            }
        }

        if self.is_file_empty(self.CLUB_FILE):
            clubs = rows  # no need to retain previous info
        else:
            with open(self.CLUB_FILE, "r", encoding="utf-8") as file:
                clubs = json.load(file)  # retaining previous info from file
                if league in clubs.keys():
                    clubs[league][season] = rows[league][
                        season
                    ]  # adding on to retained info
                else:
                    clubs[league] = rows[league]  # adding on to retained info
        return clubs

    def get_all_clubs_row_xpath(self) -> str:
        """Returns xpath for all rows containing team names

        Returns: xpath of all rows
            str: _description_
        """
        xpath = '(//table[@class="items"])[1]/tbody/tr/td[2]/a[1]'
        return xpath

    def write_to_json_file(self, json_content):
        """Writes to json in its own filepath (use case specific)

        Args:
            json_content (_type_): json content to write
        """
        super().write_to_json_file(self.CLUB_FILE, json_content)

    def have_all_leagues_seasons_club_names(self) -> bool:
        # check if req file exists
        is_club_names_file: bool = os.path.isfile(self.CLUB_FILE)
        # to check if all club names are parsed
        all_club_names_parsed: bool = is_club_names_file
        if is_club_names_file:
            with open(self.COMP_FILE, "r", encoding="utf-8") as file:
                comps = json.load(file)
                leagues = [
                    comps[country]["competitions"]["First Tier"][0]
                    for country in self.countries
                ]
            with open(self.CLUB_FILE, "r", encoding="utf-8") as file:
                clubs = json.load(file)
                all_club_names_parsed = all(
                    [
                        sorted(clubs.keys())
                        == sorted(leagues)  # every club's info exists
                        and sorted(clubs[league].keys())
                        == sorted(self.seasons)  # every year's info exists
                        for league in leagues
                    ]
                )
        return all_club_names_parsed
