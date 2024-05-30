import json
import os
import datetime
import time
import pymongo
from dotenv import load_dotenv
from pprint import pp
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


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
        self.seasons: dict = {}
        self.logger = None
        self.db_name = "football"
        # BaseClass.set_logger("spiders", None)

    def set_logger(self, logger_name: str, file_path: str):
        """Adds a logger to the class

        Args:
            logger_name (str): Name of the logger
            file_path (str): Name/path of the file
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        log_file = os.path.join(self.LOG_DIR, file_path)
        handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(module)s module - %(funcName)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        # removing logging to console by stopping the propagation to the root logger
        self.logger.propagate = False
        self.set_seasons_per_league()

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

    def set_seasons_per_league(self):
        """Creates a dict of season list for each country."""
        db = self.get_db()
        collection = db.competitions
        docs = [
            doc
            for doc in collection.find(
                filter={"current_season": {"$exists": True}}, projection={"_id": False}
            )
        ]
        if len(docs) > 0:
            self.seasons = {
                doc["country"]: sorted(
                    list(
                        set(
                            ["2018", "2019", "2020", "2021", "2022", "2023"]
                            + [
                                str(int(doc["current_season"]) - i)
                                for i in range(6, 0, -1)
                            ]
                        )
                    )
                )
                for doc in docs
            }
        else:
            self.seasons = {
                country: sorted(
                    list(
                        set(
                            ["2018", "2019", "2020", "2021", "2022", "2023"]
                            + [str(self.current_year - i) for i in range(6, 0, -1)]
                        )
                    )
                )
                for country in self.countries
            }

    def init_selenium_web_driver(self):
        chrome_options = Options()
        chrome_options.add_experimental_option("detach", True)
        driver = webdriver.Chrome(options=chrome_options)
        return driver


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
        countries: list = [c for c in cursor]
        all_country_code_parsed: bool = len(countries) > 0
        if all_country_code_parsed:
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
            db_content (list): list of dictionaries in the format [{country: code},]
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
            "First Tier Qualifying": "UEFA Champions League Qualifying",
            "Second Tier": "UEFA Europa League",
            "Second Tier Qualifying": "Europa League Qualifying",
            "Third Tier": "UEFA Europa Conference League",
            "Third Tier Qualifying": "UEFA Europa Conference League Qualifiers",
            "Cup": "UEFA Super Cup",
        }

    def write_to_json_file(self, json_content):
        """Use case specific write to json file method

        Args:
            json_content (_type_): json to write

        """
        super().write_to_json_file(self.FILE, json_content)
        self.logger.info("Written to json file.")

    def parse_domestic_comp_names(self, response, country) -> dict:
        """Parse comp names and urls for the country

        Args:
            response (_type_): spider response obj
            country (_type_): country name

        Returns:
            dict: {country: country_name, competitions:competitions_dict}
        """
        row_xpaths = (
            self.get_domestic_comps_xpaths()
        )  # row_xpaths = {tier: [title, url]}
        rows = {
            country: {
                tier: {
                    "name": response.xpath(row[0]).get(),
                    "url": response.xpath(row[1]).get(),
                }
                for tier, row in row_xpaths.items()
                if not tier == "current_season"
            }
        }
        comps: dict = {}
        comps["country"] = country
        comps["competitions"] = rows[country]
        comps["current_season"] = (
            response.xpath(row_xpaths["current_season"]).get().strip()
        )
        self.logger.debug(f"RETURNED: {comps}")
        self.logger.info("Parsed domestic competition names.")
        return comps

    def parse_intl_comp_names(self, response) -> dict:
        """Parse comp names and urls for UEFA competitions

        Args:
            response (_type_): response object from spider

        Returns:
            dict: {country: country_name, competitions: competitions_dict}
        """
        row_xpaths = self.get_intl_comps_xpath()
        rows = {
            "European": {
                "competitions": {
                    tier: {
                        "name": response.xpath(row[0]).get(),
                        "url": response.xpath(row[1]).get(),
                    }
                    for tier, row in row_xpaths.items()
                }
            }
        }
        comps: dict = {}
        comps["country"] = "Europe"
        comps["competitions"] = rows["European"]["competitions"]
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
            dict: {comp_tier:[xpath to comp title, xpath to comp url], 'current_season':xpath to current season}
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
        xpath_season = "(//table[@class='auflistung'])[1]/tbody/tr/td[2]/div/select/option[@selected='selected']/@value"
        xpaths["current_season"] = xpath_season
        self.logger.debug(f"RETURNED: {xpaths}")
        self.logger.info("Returned domestic competition xpaths.")
        return xpaths

    def have_all_domestic_comps(self) -> bool:
        """Checks if all domestic comps are parsed in the database

        Returns:
            bool: True if parsed else False
        """
        db = self.get_db()
        collection = db.competitions
        cursor = collection.find(filter={"country_code": {"$exists": True}})
        docs = [c for c in cursor]
        all_domestic_comps_parsed: bool = len(docs) > 0
        if all_domestic_comps_parsed:
            # check all countries are present, all countries have competitions, all tiers are in the competition
            countries: list = [c["country"] for c in docs]
            countries_exist: list = [c in countries for c in self.countries]
            tiers_exist: list = []
            for c in docs:
                if "competitions" in c.keys():
                    tiers_exist.append(
                        all(
                            [
                                tier in c["competitions"].keys()
                                for tier in self.competitions
                            ]
                        )
                    )
                else:
                    tiers_exist.append(False)
            all_domestic_comps_parsed = all(countries_exist + tiers_exist)
        self.logger.debug(f"RETURNED: {all_domestic_comps_parsed}")
        self.logger.info("Checked if all domestic competitions are recorded.")
        return all_domestic_comps_parsed

    def have_all_intl_comps(self) -> bool:
        """Checks if all intl (UEFA) comps are parsed in the database

        Returns:
            bool: True if parsed else False
        """
        db = self.get_db()
        collection = db.competitions
        cursor = collection.find(filter={"country": "Europe"})
        docs: list = [c for c in cursor]
        all_intl_comps_parsed: bool = len(docs) > 0
        if all_intl_comps_parsed:
            tiers_exist: list = []
            for doc in docs:
                if "competitions" in doc.keys():
                    tiers_exist.append(
                        all(
                            [
                                tier in doc["competitions"].keys()
                                for tier in self.intl_comps.keys()
                            ]
                        )
                    )
                else:
                    tiers_exist.append(False)
            all_intl_comps_parsed = all(tiers_exist)
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
        # with open(self.FILE, "r", encoding="utf-8") as file:
        #     comp_json = json.load(file)
        #     all_country_codes = {
        #         country: comp_json[country]["country_code"]
        #         for country in self.countries
        #     }
        #     all_country_urls = {
        #         country: self.get_country_url(code)
        #         for country, code in all_country_codes.items()
        #     }
        db = self.get_db()
        all_country_urls: dict = {}
        collection = db.competitions
        for country in collection.find({"country_code": {"$exists": True}}):
            all_country_urls[country["country"]] = self.get_country_url(
                country["country_code"]
            )

        self.logger.debug(f"RETURNED: {all_country_urls}")
        self.logger.info("Returned all country urls.")
        return all_country_urls

    def record_in_db(self, db_content: dict):
        """Updates database to contain competition information

        Args:
            db_content (dict): dict in format of {"country":country_name, "competitions":competitions_dict}
        """
        db = self.get_db()
        collection = db.competitions
        if db_content["country"] == "Europe":
            collection.update_many(
                filter={"country": db_content["country"]},
                update={
                    "$setOnInsert": db_content,
                },
                upsert=True,
            )
            collection.update_many(
                filter={"country": db_content["country"]},
                update={
                    "$set": db_content,
                },
            )
        else:
            collection.update_many(
                filter={
                    "$and": [
                        {"country": db_content["country"]},
                        {
                            "$or": [
                                {"competitions": {"$exists": False}},
                                {"competitions": {"$ne": db_content["competitions"]}},
                            ]
                        },
                    ]
                },
                update={
                    "$set": {
                        "competitions": db_content["competitions"],
                        "current_season": db_content["current_season"],
                    },
                },
            )

        self.logger.info("Recorded in database.")

    def update_current_seasons_in_db(self, response, country):
        """Updates the current season of country in the database.

        Args:
            response (_type_): Response object from spider
            country (_type_): Country name in string
        """
        season_xpath = self.get_domestic_comps_xpaths()["current_season"]
        current_season = response.xpath(season_xpath).get().strip()
        db = self.get_db()
        collection = db.competitions
        collection.update_one(
            filter={"country": country},
            update={"$set": {"current_season": current_season}},
        )
        self.logger.debug(f"Updated {country}'s current season as {current_season}.")
        self.logger.info(f"Updated {country}'s current season")


# class to deal to all the clubs names in all leagues and seasons
class ClubNames(BaseClass):
    def __init__(self):
        super().__init__()
        self.COMP_FILE = os.path.join(self.DATA_DIR, "competitions.json")
        self.CLUB_FILE = os.path.join(self.DATA_DIR, "clubs.json")
        self.LOG_FILE = os.path.join(self.LOG_DIR, "club_names.log")
        self.set_logger("club_names", self.LOG_FILE)
        self.logger.debug(f"SELF.SEASONS: {self.seasons}")

    def get_comp_url(self, country: str, comp: str, season: str) -> dict:
        """Provides the competition url based on country and competition

        Args:
            country (str): Country name
            comp (str): Competition Type

        Returns:
            str: {url:url of that competition, league:league name, season:season start year}
        """
        db = self.get_db()
        collection = db.competitions
        doc = collection.find_one({"country": country})
        url: str = (
            "https://www.transfermarkt.com"
            + doc["competitions"][comp]["url"]
            + "/plus/?saison_id="
            + season
        )
        url_info = {
            "url": url,
            "league": doc["competitions"][comp]["name"],
            "season": season,
        }
        self.logger.debug(f"RETURNED: {url_info}")
        self.logger.info(
            f"Returned {country}'s {comp} competition's url for {season} season."
        )
        return url_info

    def get_all_seasons_leagues_url(self) -> list[str]:
        """Provides league urls for all concerned countries

        Returns:
            list[str]: list of urls
        """
        league_urls = [
            self.get_comp_url(country, "First Tier", season)
            for country in self.countries
            for season in self.seasons[country]
        ]
        self.logger.debug(f"RETURNED: {league_urls}")
        self.logger.info("Returned the urls of all leagues for all the seasons.")
        return league_urls

    def parse_club_names(self, response, league, season) -> dict:
        """Parses club names, urls and returns them

        Args:
            response (_type_): response obj from spider
            league (_type_): name of league
            season (_type_): start year of season

        Returns:
            dict: {league:league_name, season: season_start, clubs: [{name: club_name, url: club_url}]}
        """
        all_clubs_rows_xpath = self.get_all_clubs_row_xpath()
        rows = response.xpath(all_clubs_rows_xpath)
        rows = {
            "league": league,
            "season": season,
            "clubs": [
                {
                    "name": row.xpath("text()").get(),
                    "url": row.xpath("@href[1]").get(),
                }
                for row in rows
            ],
        }
        clubs = rows
        self.logger.debug(f"RETURNED: {clubs}")
        self.logger.info("Parsed club names.")
        return clubs

    def get_all_clubs_row_xpath(self) -> str:
        """Returns xpath for all rows containing team names

        Returns: xpath of all rows
            str: _description_
        """
        xpath = '(//table[@class="items"])[1]/tbody/tr/td[2]/a[1]'
        self.logger.debug(f"RETURNED: {xpath}")
        self.logger.info("Returned xpath for all club names.")
        return xpath

    def write_to_json_file(self, json_content):
        """Writes to json in its own filepath (use case specific)

        Args:
            json_content (_type_): json content to write
        """
        self.logger.info("Written to json file.")
        super().write_to_json_file(self.CLUB_FILE, json_content)

    def record_in_db(self, data: dict):
        """Records all the clubs info as well as season wise club names for all the leagues.

        Args:
            data (dict): Dict containing info about the season wise club list for all the leagues.
        """
        self.record_club_info_in_db(
            season=data["season"], data=data["clubs"]
        )  # storing all club info in a single database
        self.record_league_clubs_in_db(data)

    def record_club_info_in_db(self, season: str, data: list):
        """Records all the clubs within a single collection in the database

        Args:
            season (str): Season start year
            data (list): List conatining dicts of clubs and their season wise urls.
        """
        db = self.get_db()
        collection = db.all_clubs
        write_reqs = (
            # if club name doesn't exist, then insert
            [
                pymongo.UpdateOne(
                    filter={"name": club["name"]},
                    update={
                        "$setOnInsert": {
                            "name": club["name"],
                            "code": club["url"].split("/")[-3],
                            "urls": {season: club["url"]},
                        }
                    },
                    upsert=True,
                )
                for club in data
            ]
            # if club name exists then update
            + [
                pymongo.UpdateOne(
                    filter={"name": club["name"]},
                    update={"$set": {f"urls.{season}": club["url"]}},
                )
                for club in data
            ]
        )
        collection.bulk_write(write_reqs)
        self.logger.info("Recorded club names and urls in database.")

    def record_league_clubs_in_db(self, data: dict):
        """Records all clubs in a league in a respective season in the database.

        Args:
            data (dict): dict containing info of league name, season, and all the clubs.
        """
        db = self.get_db()
        all_clubs = db.all_clubs
        clubs = [
            all_clubs.find_one(filter={"name": club["name"]}) for club in data["clubs"]
        ]
        club_names = [club["name"] for club in clubs]
        collection = db.all_leagues
        write_reqs = (
            # if no document of that league is found
            [
                pymongo.UpdateOne(
                    filter={"name": data["league"]},
                    update={
                        "$setOnInsert": {
                            "name": data["league"],
                            f"clubs.{data['season']}": club_names,
                        }
                    },
                    upsert=True,
                )
            ]
            # if the document of that league is found
            + [
                pymongo.UpdateOne(
                    filter={"name": data["league"]},
                    update={"$set": {f"clubs.{data['season']}": club_names}},
                )
            ]
        )
        collection.bulk_write(write_reqs)
        self.logger.info(
            f"Recorded the clubs in {data['league']} for {data['season']} season."
        )

    def have_all_leagues_seasons_club_names(self) -> bool:
        """Checks if club names for all the leagues in all seasons are stored in the database.

        Returns:
            bool: True if all club names for all seasons found in the record, else False.
        """
        db = self.get_db()
        collection = db.all_leagues
        docs = [c for c in collection.find(projection={"_id": False})]
        all_club_names_parsed: bool = len(docs) > 0
        if all_club_names_parsed:
            seasons_exist: list = []
            for doc in docs:
                seasons_parsed: list = doc["clubs"].keys()
                doc_country = db.competitions.find_one(
                    filter={"competitions.First Tier.name": doc["name"]}
                )["country"]
                seasons_exist.append(
                    all(
                        [
                            season in seasons_parsed
                            for season in self.seasons[doc_country]
                        ]
                    )
                )
            leagues_parsed: list = [doc["name"] for doc in docs]
            league_names = [
                c["competitions"]["First Tier"]["name"]
                for c in db.competitions.find(projection={"_id": False})
                if not c["country"] == "Europe"
            ]
            leagues_exist = [league in leagues_parsed for league in league_names]
            all_club_names_parsed = all(leagues_exist + seasons_exist)
        self.logger.debug(f"RETURNED: {all_club_names_parsed}")
        self.logger.info("Checked if all the clubs for all seasons are parsed.")
        return all_club_names_parsed


class Fixtures(BaseClass):
    def __init__(self):
        super().__init__()
        self.LOG_FILE = os.path.join(self.LOG_DIR, "fixtures.log")
        self.set_logger("fixtures", self.LOG_FILE)

    def get_club_fixture_url(self, club_name: str, season: str) -> str:
        """Given a club name and its season, returns the url for the fixtures of the club in that season.

        Args:
            club_name (str): Name of the club
            season (str): Start year of season

        Returns:
            str: required url
        """
        db = self.get_db()
        collection = db.all_clubs
        doc = collection.find_one(filter={"name": club_name})
        url_club_name = doc["urls"][season].split("/")[-6]
        club_code = doc["code"]
        url = f"https://www.transfermarkt.com/{url_club_name}/spielplandatum/verein/{club_code}/plus/0?saison_id={season}"
        self.logger.debug(f"RETURNED: {url}")
        self.logger.info(f"Returned fixture url for {club_name}'s {season} season")
        return url

    def get_all_club_all_season_fixture_urls(self) -> dict:
        """Returns the url for the fixtures of all the clubs in all seasons from the database.

        Returns:
            dict: {league_name:[{'team':club_name, season: season_start_year 'url':club_fixture_url}]}
        """
        db = self.get_db()
        collection = db.all_leagues
        docs = [c for c in collection.find(projection={"_id": False})]
        urls: dict = {}
        for doc in docs:
            urls[doc["name"]] = []
            for season, clubs in doc["clubs"].items():
                urls[doc["name"]] += [
                    {
                        "team": club,
                        "season": season,
                        "url": self.get_club_fixture_url(club, season),
                    }
                    for club in clubs
                ]
        self.logger.debug(f"RETURNED: {urls}")
        self.logger.info("Returned the urls for all the fixtures.")
        return urls

    def get_all_fixtures_xpath(self):
        """Generates xpath for the fixtures listed in a table.

        Returns:
            str: xpath
        """
        xpath_fixtures = "(//table[not(@class='auflistung')])[1]/tbody/tr[@style]"
        self.logger.debug(f"RETURNED: {xpath_fixtures}")
        self.logger.info("Returned the xpath for all the fixtures in the table.")
        return xpath_fixtures

    def parse_all_fixtures_info(self, response, team, season) -> dict:
        """Parses info from all the rows within a fixture table.

        Args:
            response (_type_): response obj from the spider
            team (str): Name of the team whose fixture is being parsed

        Returns:
            dict: Returns fixture info dict in the format of {'team':team, 'fixtures':[{fixture}]}
        """
        rows_xpath = self.get_all_fixtures_xpath()
        fixture_info = {}
        fixture_info["team"] = team
        fixture_info["season"] = season
        fixture_info["fixtures"] = []
        fixture_rows = response.xpath(rows_xpath)
        for row in fixture_rows:
            fixture = {}
            fix_date = row.xpath("td[2]/text()").get()
            # checking if postponed
            if fix_date.strip().lower() == "unknown":
                continue
            # if not upcoming match
            if not (
                datetime.date(
                    int("20" + fix_date.split(".")[3].strip()),
                    int(fix_date.split(".")[2].strip()),
                    int(fix_date.split(".")[1].strip()),
                )
                >= datetime.date.today()
            ):
                fixture["match_status"] = "PLAYED"
                fixture["competition"] = row.xpath(
                    "preceding-sibling::tr[not(@style)][1]/td/a/@title"
                ).get()
                fixture["date"] = str(
                    datetime.date(
                        int("20" + fix_date.split(".")[3].strip()),
                        int(fix_date.split(".")[2].strip()),
                        int(fix_date.split(".")[1].strip()),
                    )
                )
                fixture["day"] = fix_date.split(".")[0].strip()
                fixture["time"] = row.xpath("td[3]/text()").get().strip()
                fixture["venue"] = row.xpath("td[4]/text()").get().strip()
                fixture["matchday_rank"] = (
                    "N/A"
                    if row.xpath("td[5]/span/text()").get() is None
                    else row.xpath("td[5]/span/text()").get().strip()
                )
                fixture["opponent_team"] = row.xpath("td[7]/a/@title").get()
                fixture["opponent_matchday_rank"] = (
                    "N/A"
                    if row.xpath("td[7]/span/text()").get() is None
                    else row.xpath("td[7]/span/text()").get().strip()
                )
                fixture["goals_scored"] = (
                    row.xpath("td[10]/a/span/text()").get().strip().split(":")[0]
                    if fixture["venue"].lower() == "h"
                    else row.xpath("td[10]/a/span/text()").get().strip().split(":")[1]
                )
                fixture["goals_conceded"] = (
                    row.xpath("td[10]/a/span/text()").get().strip().split(":")[1]
                    if fixture["venue"].lower() == "h"
                    else row.xpath("td[10]/a/span/text()").get().strip().split(":")[0]
                )
                fixture["result"] = (
                    "Won"
                    if row.xpath("td[10]/a/span/@class").get().strip().lower()
                    == "greentext"
                    else "Lost"
                    if row.xpath("td[10]/a/span/@class").get().strip().lower()
                    == "redtext"
                    else "draw"
                )
                pens = (
                    row.xpath("td[10]/a/span/span/text()").get().strip().lower()
                    if row.xpath("td[10]/a/span/span").get() is not None
                    else None
                )
                fixture["on_pens"] = (
                    False if pens is None else False if not "on pens" == pens else True
                )
            else:
                fixture["match_status"] = "UPCOMING"
                fixture["competition"] = row.xpath(
                    "preceding-sibling::tr[not(@style)][1]/td/a/@title"
                ).get()
                fixture["date"] = str(
                    datetime.date(
                        int("20" + fix_date.split(".")[3].strip()),
                        int(fix_date.split(".")[2].strip()),
                        int(fix_date.split(".")[1].strip()),
                    )
                )
                fixture["day"] = fix_date.split(".")[0].strip()
                fixture["time"] = row.xpath("td[3]/text()").get().strip()
                fixture["venue"] = row.xpath("td[4]/text()").get().strip()
                fixture["matchday_rank"] = "TBD"
                fixture["opponent_team"] = row.xpath("td[7]/a/@title").get()
                fixture["opponent_matchday_rank"] = "TBD"
                fixture["goals_scored"] = "TBD"
                fixture["goals_conceded"] = "TBD"
            fixture_info["fixtures"].append(fixture)

        self.logger.debug(f"RETURNED: {fixture_info}")
        self.logger.info(f"Returned fixtures of {team}.")
        return fixture_info

    def record_fixtures_in_db(self, fixture_info):
        """Records fixture_info in database

        Args:
            fixture_info (dict): parsed fixture info
        """
        db = self.get_db()
        played = db.played_fixtures
        upcoming = db.upcoming_fixtures
        for fixture in fixture_info["fixtures"]:
            if fixture["match_status"] == "PLAYED":
                # insert doc if doc not found
                played.update_one(
                    filter={
                        "club": fixture_info["team"],
                    },
                    update={
                        "$setOnInsert": {
                            "club": fixture_info["team"],
                            f"seasons.{fixture_info['season']}.{fixture['competition']}": [
                                {
                                    "date": fixture["date"],
                                    "day": fixture["day"],
                                    "time": fixture["time"],
                                    "venue": fixture["venue"],
                                    "matchday_rank": fixture["matchday_rank"],
                                    "opponent": fixture["opponent_team"],
                                    "opponent_matchday_rank": fixture[
                                        "opponent_matchday_rank"
                                    ],
                                    "goals_scored": fixture["goals_scored"],
                                    "goals_conceded": fixture["goals_conceded"],
                                    "result": fixture["result"],
                                    "on_pens": fixture["on_pens"],
                                }
                            ],
                        }
                    },
                    upsert=True,
                )
                # push fixture if doc found
                played.update_one(
                    filter={
                        "club": fixture_info["team"],
                        f"seasons.{fixture_info['season']}.{fixture['competition']}": {
                            "$nin": [
                                {
                                    "date": fixture["date"],
                                    "day": fixture["day"],
                                    "time": fixture["time"],
                                    "venue": fixture["venue"],
                                    "matchday_rank": fixture["matchday_rank"],
                                    "opponent": fixture["opponent_team"],
                                    "opponent_matchday_rank": fixture[
                                        "opponent_matchday_rank"
                                    ],
                                    "goals_scored": fixture["goals_scored"],
                                    "goals_conceded": fixture["goals_conceded"],
                                    "result": fixture["result"],
                                    "on_pens": fixture["on_pens"],
                                }
                            ]
                        },
                    },
                    update={
                        "$push": {
                            f"seasons.{fixture_info['season']}.{fixture['competition']}": {
                                "date": fixture["date"],
                                "day": fixture["day"],
                                "time": fixture["time"],
                                "venue": fixture["venue"],
                                "matchday_rank": fixture["matchday_rank"],
                                "opponent": fixture["opponent_team"],
                                "opponent_matchday_rank": fixture[
                                    "opponent_matchday_rank"
                                ],
                                "goals_scored": fixture["goals_scored"],
                                "goals_conceded": fixture["goals_conceded"],
                                "result": fixture["result"],
                                "on_pens": fixture["on_pens"],
                            }
                        }
                    },
                )
                # delete the same fixture from upcoming if applicable
                upcoming.update_one(
                    filter={"club": fixture_info["team"]},
                    update={
                        "$pull": {
                            f"seasons.{fixture_info['season']}.{fixture['competition']}": {
                                "date": fixture["date"],
                                "day": fixture["day"],
                                "time": fixture["time"],
                                "venue": fixture["venue"],
                                "opponent": fixture["opponent_team"],
                            }
                        }
                    },
                )
                col = "played_fixtures"
                self.logger.info(
                    f"Recorded {fixture_info['team']} fixture in {col} collection."
                )

            elif fixture["match_status"] == "UPCOMING":
                # insert new document if no document match
                upcoming.update_one(
                    filter={"club": fixture_info["team"]},
                    update={
                        "$setOnInsert": {
                            "club": fixture_info["team"],
                            f"seasons.{fixture_info['season']}.{fixture['competition']}": [
                                {
                                    "date": fixture["date"],
                                    "day": fixture["day"],
                                    "time": fixture["time"],
                                    "venue": fixture["venue"],
                                    "opponent": fixture["opponent_team"],
                                }
                            ],
                        }
                    },
                    upsert=True,
                )
                # push new fixture if document match
                upcoming.update_one(
                    filter={
                        "club": fixture_info["team"],
                        f"seasons.{fixture_info['season']}.{fixture['competition']}": {
                            "$nin": [
                                {
                                    "date": fixture["date"],
                                    "day": fixture["day"],
                                    "time": fixture["time"],
                                    "venue": fixture["venue"],
                                    "opponent": fixture["opponent_team"],
                                }
                            ]
                        },
                    },
                    update={
                        "$push": {
                            f"seasons.{fixture_info['season']}.{fixture['competition']}": {
                                "date": fixture["date"],
                                "day": fixture["day"],
                                "time": fixture["time"],
                                "venue": fixture["venue"],
                                "opponent": fixture["opponent_team"],
                            }
                        }
                    },
                )
                col = "upcoming_fixtures"
                self.logger.info(
                    f"Recorded {fixture_info['team']} fixture in {col} collection."
                )
                # no need to delete from played_fixtures because upcoming match won't be stored there

    def have_all_fixtures(self) -> bool:
        """Checks if all the fixtures of all the clubs for relevant seasons are parsed.

        Returns:
            bool: True if all fixtures are passed else False
        """
        # we'll only check in played fixtures because the upcoming fixtures are supposed to be parsed regularly
        db = self.get_db()
        played = db.played_fixtures
        upcoming = db.upcoming_fixtures
        played_docs = [doc for doc in played.find(projection={"_id": False})]
        have_all_fixtures_parsed: bool = len(played_docs) > 0
        if have_all_fixtures_parsed:
            played_docs_count = len(played_docs)
            all_clubs_count = len(
                [doc for doc in db.all_clubs.find(projection={"_id": False})]
            )
            all_upcoming_fixtures_count = len(
                [doc for doc in upcoming.find(projection={"_id": False})]
            )
            clubs_out_of_current_season_count: int = 0
            for doc in played_docs:
                all_seasons: list = []
                for _, v in self.seasons.items():
                    all_seasons += v
                all_seasons = set(all_seasons)
                queries = [{f"clubs.{season}": doc["club"]} for season in all_seasons]
                doc_league = db.all_leagues.find_one(filter={"$or": queries})["name"]
                doc_country_current_season = db.competitions.find_one(
                    filter={"competitions.First Tier.name": doc_league}
                )["current_season"]
                if doc_country_current_season not in doc["seasons"].keys():
                    clubs_out_of_current_season_count += 1
            have_all_fixtures_parsed = (played_docs_count == all_clubs_count) and (
                played_docs_count - clubs_out_of_current_season_count
                == all_upcoming_fixtures_count
            )
            self.logger.debug(f"PLAYED DOCS COUNT: {played_docs_count}")
            self.logger.debug(f"ALL CLUBS COUNT: {all_clubs_count}")
            self.logger.debug(
                f"CLUBS OUT OF CURRENT SEASON COUNT: {clubs_out_of_current_season_count}"
            )
            self.logger.debug(
                f"ALL UPCOMING FIXTURES COUNT: {all_upcoming_fixtures_count}"
            )
        self.logger.debug(f"RETURNED: {have_all_fixtures_parsed}")
        self.logger.info(
            "Checked if all the fixtures of all the clubs are recorded in the database."
        )
        return have_all_fixtures_parsed

    def cleanup_upcoming_empty_docs(self):
        """Scans through the collection upcoming_fixtures to unset empty fields and delete documents with no upcoming features"""
        db = self.get_db()
        collection = db.upcoming_fixtures
        docs: list = [doc for doc in collection.find(projection={"_id": False})]
        cleaned_fields = 0
        cleaned_docs = 0
        for doc in docs:
            seasons = doc["seasons"]
            if len(seasons) > 0:
                for season_year, season in seasons.items():
                    if len(season) > 0:
                        for comp_name, fixtures in season.items():
                            # unsetting competition fields if empty
                            if len(fixtures) < 1 or fixtures is None:
                                collection.update_one(
                                    filter={"club": doc["club"]},
                                    update={
                                        "$unset": {
                                            f"seasons.{season_year}.{comp_name}": ""
                                        }
                                    },
                                )
                                self.logger.info(
                                    f"Unset the field: seasons.{season_year}.{comp_name} of club {doc['club']}."
                                )
                                cleaned_fields += 1
                                season.pop(comp_name)
                    # unsetting seasons if empty
                    if len(season) < 1:
                        collection.update_one(
                            filter={"club": doc["club"]},
                            update={"$unset": {f"seasons.{season_year}": ""}},
                        )
                        self.logger.info(
                            f"Unset the field: seasons.{season_year} of club {doc['club']}."
                        )
                        cleaned_fields += 1
                        seasons.pop(season_year)
            # deleting doc if no seasons
            if len(seasons) < 1:
                collection.delete_one(filter={"club": doc["club"]})
                self.logger.info(
                    f"Deleted {doc['club']} club's document from the upcoming_fixtures collection."
                )
                cleaned_docs += 1
        self.logger.info(
            "Checked the upcoming_fixtures collection and cleaned documents if applicable."
        )
        self.logger.info(
            f"Cleaned fields: {cleaned_fields}. Deleted docs: {cleaned_docs}"
        )

    def get_all_club_current_season_urls(self) -> dict:
        """Fetches the urls for all the clubs for the current season

        Returns:
            dict: dict: {league_name:[{'team':club_name, season: season_start_year 'url':club_fixture_url}]}
        """
        db = self.get_db()
        collection = db.all_leagues
        docs = [c for c in collection.find(projection={"_id": False})]
        urls: dict = {}
        for doc in docs:
            urls[doc["name"]] = []
            current_season = db.competitions.find_one(
                filter={"competitions.First Tier.name": doc["name"]}
            )["current_season"]

            for club in doc["clubs"][current_season]:
                urls[doc["name"]] += [
                    {
                        "team": club,
                        "season": current_season,
                        "url": self.get_club_fixture_url(club, current_season),
                    }
                ]
        self.logger.debug(f"RETURNED: {urls}")
        self.logger.info(
            "Returned the urls for all the fixtures of the current season."
        )
        return urls

    def reset_current_season_fixtures(self):
        """Resets the data stored about current season fixtures."""
        db = self.get_db()
        played = db.played_fixtures
        upcoming = db.upcoming_fixtures
        # resetting played_fixtures for current season
        played_docs = [doc for doc in played.find(projection={"_id": False})]
        for doc in played_docs:
            all_seasons: list = []
            for _, v in self.seasons.items():
                all_seasons += v
            all_seasons = set(all_seasons)
            queries = [{f"clubs.{season}": doc["club"]} for season in all_seasons]
            doc_league = db.all_leagues.find_one(filter={"$or": queries})["name"]
            doc_current_season = db.competitions.find_one(
                filter={"competitions.First Tier.name": doc_league}
            )["current_season"]
            if doc_current_season in doc["seasons"].keys():
                played.update_one(
                    filter={"club": doc["club"]},
                    update={"$unset": {f"seasons.{doc_current_season}": ""}},
                )
        # resetting upcoming_fixtures for current season
        upcoming_docs = [doc for doc in upcoming.find(projection={"_id": False})]
        for doc in upcoming_docs:
            all_seasons: list = []
            for _, v in self.seasons.items():
                all_seasons += v
            all_seasons = set(all_seasons)
            queries = [{f"clubs.{season}": doc["club"]} for season in all_seasons]
            doc_league = db.all_leagues.find_one(filter={"$or": queries})["name"]
            doc_current_season = db.competitions.find_one(
                filter={"competitions.First Tier.name": doc_league}
            )["current_season"]
            if doc_current_season in doc["seasons"].keys():
                upcoming.update_one(
                    filter={"club": doc["club"]},
                    update={"$unset": {f"seasons.{doc_current_season}": ""}},
                )
        self.logger.info(
            "Reset current season fixtures for both played_fixtures collection and upcoming_fixtures collection."
        )


class Injuries(BaseClass):
    def __init__(self):
        super().__init__()
        self.LOG_FILE = os.path.join(self.LOG_DIR, "injuries.log")
        self.set_logger("injuries", self.LOG_FILE)

    def check_if_fixture_injuries_parsed(self, team: str, date: str) -> bool:
        """Checks if the injuries are parsed for a fixture, given a team and a date

        Args:
            team (str): the name of a team playing in a fixture
            date (str): the date of the fixture

        Returns:
            bool: True if injuries parsed else False.
        """
        db = self.get_db()
        collection = db.injuries
        doc = collection.find_one(
            filter={
                "club": team,
                "$or": [
                    {
                        f"played.{date}": {"$exists": True}
                    },  # check if the injuries is parsed under played fixtures
                    {
                        f"upcoming:{date}": {"$exists": True}
                    },  # check if the injuries is parsed under upcoming fixtures
                ],
            }
        )

        injuries_parsed: bool = True if doc is not None else False

        self.logger.debug(f"RETURNED: {injuries_parsed}")
        self.logger.info(f"Checked if injuries parsed for {team}'s fixture in {date}.")
        return injuries_parsed

    def get_missing_injuries_played(self) -> list[dict]:
        """Gets all the fixtures whose injuries have not been parsed

        Returns:
            list[dict]: List of fixtures whose injuries have not been parsed.[{club:club_name, date:date}]
        """
        db = self.get_db()
        collection = db.played_fixtures
        docs = [doc for doc in collection.find(projection={"_id": False})]
        missing_injuries_played: list = []
        # TODO0:remove limit
        for doc in docs[:5]:
            team_name = doc["club"]
            for season, season_info in doc["seasons"].items():
                # 2017, {comp:[fixtures]}
                for comp_name, fixtures in season_info.items():
                    # UCL, [fixtures]
                    for fixture in fixtures:
                        date = fixture["date"]
                        if not self.check_if_fixture_injuries_parsed(team_name, date):
                            missing_injuries_played.append(
                                {"club": team_name, "date": date}
                            )

        self.logger.debug(f"RETURNED: {missing_injuries_played}")
        self.logger.info("Checked if all played fixtures have missing players.")
        # TODO0:remove limit
        return [missing_injuries_played[0]]

    def check_if_api_team_id_parsed(self, team: str) -> bool:
        """Checks if the team_id for API is parsed.

        Args:
            team (str): Name of the team.

        Returns:
            bool: True if team_id available else False.
        """
        db = self.get_db()
        collection = db.injuries
        doc = collection.find_one(
            filter={"club": team, "api_team_id": {"$exists": True}}
        )
        api_team_id_parsed: bool = True if doc is not None else False
        self.logger.debug(f"RETURNED: {api_team_id_parsed}")
        self.logger.info(f"Checked if API team_id available for {team}.")

        return api_team_id_parsed

    def get_missing_api_team_id(self) -> list[str]:
        db = self.get_db()
        collection = db.played_fixtures
        docs = [doc for doc in collection.find(projection={"_id": False})]
        missing_api_team_id: list = []
        # TODO0: remove limit
        for doc in docs[:5]:
            team_name = doc["club"]
            if not self.check_if_api_team_id_parsed(team_name):
                missing_api_team_id.append(team_name)

        self.logger.debug(f"RETURNED: {missing_api_team_id}")
        self.logger.info("Returned a list of teams that have missing API team_id.")
        # TODO0: remove limit
        return missing_api_team_id

    def parse_team_id(self):
        # opening login page
        driver = self.init_selenium_web_driver()
        url = "https://dashboard.api-football.com/login"
        driver.get(url)
        driver.maximize_window()
        # clicking captcha
        captcha = driver.find_element(By.XPATH, "//iframe[@title='reCAPTCHA']")
        time.sleep(2)
        captcha.click()
        time.sleep(2)
        # credential filling
        email_input = driver.find_element(By.XPATH, "//input[@name='email']")
        pw_input = driver.find_element(By.XPATH, "//input[@name='pass']")
        email_input.send_keys(os.getenv("LOGIN_EMAIL"))
        pw_input.send_keys(os.getenv("LOGIN_PW"))
        # clicking on sign in
        submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        submit_button.click()
        # go to ids url
        driver.get("https://dashboard.api-football.com/soccer/ids/teams/England")
        table_length_select = driver.find_element(
            By.XPATH,
            "//div[@id='dataTable_wrapper']/div[1]/div[1]/div/label/select/option[@value='100']",
        )
        table_length_select.click()
        # access the table with ids
        id_table_rows = driver.find_elements(
            By.XPATH,
            "//table[@id='dataTable']/tbody/tr",
        )
        self.logger.debug(f"No. of rows: {len(id_table_rows)}")

    # needed for injuries: date (YYYY-MM_DD), team_id


# create separate injury collection (club: played/upcoming: seasons: comps: fixture-wise injuries)
# played injuries will be obtained from API
# upcoming injuries will be obtained from webpages
# only upcoming injuries will be reset, played injuries will remain intact
# a method to check whether a given fixture (date, club) has injuries parsed
# TODO1: scrape team_id from API documentation dashboard
# TODO (Sure): injuries, transfers, suspensions
# TODO (Optional): squad age
