import json
import os
import datetime
import pymongo
from dotenv import load_dotenv
from pprint import pp
import logging

# TODO 1: look into logging
# TODO 2: write data onto db instead of json for competitions and club names


class BaseClass:
    def __init__(self):
        self.DATA_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../../data")
        )
        self.LOG_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../../logs/spiders")
        )
        load_dotenv()
        self.mongo_con = os.getenv("MONGODB_CLIENT", "mongodb://localhost:27017")
        self.countries = ["England", "Spain", "Italy", "Germany", "France"]
        self.competitions = [
            "First Tier",
            "Domestic Cup",
            "Domestic Super Cup",
            "League Cup",
        ]
        self.current_year = datetime.datetime.now().year
        self.seasons = sorted(
            list(
                set(
                    ["2018", "2019", "2020", "2021", "2022", "2023"]
                    + [str(self.current_year - i) for i in range(6, 0, -1)]
                )
            )
        )
        self.logger = None
        self.db_name = "football"

    def set_logger(self, logger_name: str, file_path: str):
        """Adds a logger to the class

        Args:
            logger_name (str): Name of the logger
            file_path (str): Name/path of the file
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(file_path, mode="w", encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(module)s module - %(funcName)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

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

    def get_db(self, db_name: str = None):
        """Returns db object

        Args:
            db_name (str, optional): Name of the required database. Defaults to None.

        Returns:
            _type_: db_object
        """
        if db_name is None:
            db_name = self.db_name
        mongo_client = pymongo.MongoClient(self.mongo_con)
        return mongo_client[db_name]


# class for dealing with country codes while obtaining data
class CountryCodes(BaseClass):
    def __init__(self):
        super().__init__()
        self.FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "country_codes.log")
        self.set_logger("country", self.LOG_FILE)

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
        self.logger.debug(f"xpath: {xpath}")
        self.logger.info("Returned xpath.")
        return xpath

    def parse_country_codes(self, response) -> list:
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
        competitions: list = []
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
            competitions.append({"country": country, "country_code": country_code})
        self.logger.debug(f"RETURNED: {competitions}")
        self.logger.info("Parsed country codes.")
        return competitions

    def have_all_country_codes(self) -> bool:
        """Checks whether all the county codes are parsed in the collection

        Returns:
            bool: True if all codes are parsed else False
        """
        db = self.get_db()
        collection = db.competitions
        # delete if a document lacking "country" key because the check condition below expects a "country" key;
        collection.delete_many(filter={"country": {"$exists": False}})
        cursor = collection.find(
            filter={"country_code": {"$exists": True}}, projection={"_id": False}
        )
        all_country_code_parsed: bool = cursor.alive
        if all_country_code_parsed:
            countries: list = [c for c in cursor]
            keys: list = [country["country"] for country in countries]
            all_country_code_parsed = all(
                [country in keys for country in self.countries]
            )
        self.logger.debug(f"RETURNED: {all_country_code_parsed}")
        self.logger.info("Checked if all the country codes are recorded.")
        return all_country_code_parsed

    def write_to_json_file(self, json_content):
        """Writes to json in its own filepath (use case specific)

        Args:
            json_content (_type_): json content to write
        """
        super().write_to_json_file(self.FILE, json_content)
        self.logger.info("Written to json file.")

    def record_in_db(self, db_content: list):
        """Records the data in db by completing incomplete data, or adding missing data

        Args:
            db_content (list): list of dictionaries in the format {country: code}
        """
        db = self.get_db()
        collection = db.competitions
        update_reqs = (
            # only has country, no code
            [
                pymongo.UpdateOne(
                    filter={
                        "country": data["country"],
                        "country_code": {"$exists": False},
                    },
                    update={"$set": data},
                )
                for data in db_content
            ]
            # stored code doesn't match parsed code
            + [
                pymongo.UpdateOne(
                    filter={
                        "country": data["country"],
                        "country_code": {"$ne": False},
                    },
                    update={"$set": {"country_code": data["country_code"]}},
                )
                for data in db_content
            ]
            # only has code, no country
            + [
                pymongo.UpdateOne(
                    filter={
                        "country": {"$exists": False},
                        "country_code": data["country_code"],
                    },
                    update={"$set": data},
                )
                for data in db_content
            ]
            # has no country, no code
            + [
                pymongo.UpdateOne(
                    filter={
                        "country": data["country"],
                        "country_code": data["country_code"],
                    },
                    update={"$setOnInsert": data},
                    upsert=True,
                )
                for data in db_content
            ]
        )
        collection.bulk_write(update_reqs)
        self.logger.info("Recorded in database.")


# class for dealing with competition names while obtaining data
class CompetitionNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "competitions.log")
        self.set_logger("competitions", self.LOG_FILE)
        self.intl_comps = {
            "First Tier": "UEFA Champions League",
            "Second Tier": "UEFA Europa League",
            "Third Tier": "UEFA Europa Conference League",
            "Cup": "UEFA Super Cup",
        }

    def write_to_json_file(self, json_content):
        """Use case specific write to json file method

        Args:
            json_content (_type_): json to write

        """
        super().write_to_json_file(self.FILE, json_content)
        self.logger.info("Written to json file.")

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
        self.logger.debug(f"RETURNED: {comps}")
        self.logger.info("Parsed domestic competition names.")
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
        self.logger.debug(f"RETURNED: {comps}")
        self.logger.info("Parsed intl competition names.")
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
        self.logger.debug(f"RETURNED: {xpaths}")
        self.logger.info("Returned intl competition xpath.")
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
        self.logger.debug(f"RETURNED: {xpaths}")
        self.logger.info("Returned domestic competition xpaths.")
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
        self.logger.debug(f"RETURNED: {all_domestic_comps_parsed}")
        self.logger.info("Checked if all domestic competitions are recorded.")
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
        self.logger.debug(f"RETURNED: {all_intl_comps_parsed}")
        self.logger.info("Checked if all intl competitions are recorded.")
        return all_intl_comps_parsed

    def get_country_url(self, country_code) -> str:
        """Forms a country URL by appending country code

        Args:
            country_code (_type_): country code

        Returns:
            str: _description_
        """
        url: str = f"https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/{country_code}"
        self.logger.debug(f"RETURNED: {url}")
        self.logger.info("Returned country URL.")
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
        self.logger.debug(f"RETURNED: {all_country_urls}")
        self.logger.info("Returned all country urls.")
        return all_country_urls


# class to deal to all the clubs names in all leagues and seasons
class ClubNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.COMP_FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.CLUB_FILE = os.path.join(self.DATA_DIR, "clubs.json")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "club_names.log")
        self.set_logger("club_names", self.LOG_FILE)

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
