import json
import os
import datetime
import pymongo
from dotenv import load_dotenv
from pprint import pp
import logging


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
            }
        }
        comps: dict = {}
        comps["country"] = country
        comps["competitions"] = rows[country]
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
                    "$set": {"competitions": db_content["competitions"]},
                },
            )

        self.logger.info("Recorded in database.")


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
            for season in self.seasons
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
                seasons_exist.append(
                    all([season in seasons_parsed for season in self.seasons])
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

    def get_all_club_all_season_fixture_urls(self) -> list[dict]:
        """Returns the url for the fixtures of all the clubs in all seasons from the database.

        Returns:
            list[dict]: [{league_name: [urls]},]
        """
        db = self.get_db()
        collection = db.all_leagues
        docs = [c for c in collection.find(projection={"_id": False})]
        urls: dict = {}
        for doc in docs:
            urls[doc["name"]] = []
            for season, clubs in doc["clubs"].items():
                urls[doc["name"]] += [
                    self.get_club_fixture_url(club, season) for club in clubs
                ]
        self.logger.debug(f"RETURNED: {urls}")
        self.logger.info("Returned the urls for all the fixtures.")
        return urls

    def get_all_fixtures_xpath(self):
        xpath_fixtures = "(//table[not(@class='auflistung')])[1]/tbody/tr[@style]"
        xpath_comps = (
            f"{xpath_fixtures}/preceding-sibling::tr[not(@style)][1]/td/a/@title"
        )
        return {"comp_xpath": xpath_comps, "fixtures_xpath": xpath_fixtures}

    def parse_fixtures_info(self, response):
        rows_xpath = self.get_all_fixtures_xpath()
        fixture_info = {}
        fixture_info["competition"] = response.xpath(rows_xpath["comp_xpath"]).get()
        fixture_rows = response.xpath(rows_xpath["fixtures_xpath"])
        # TODO1: figure out and parse the non-opponent club
        # TODO2: parse until the match day date exceeds the current day
        # TODO3: verify that the competitions are accurate to fixtures
        for row in fixture_rows:
            fix_date = row.xpath("td[2]/text()").get()
            fixture_info["date"] = (
                fix_date.split(".")[1].strip()
                + fix_date.split(".")[2].strip()
                + fix_date.split(".")[3].strip()
            )
            fixture_info["day"] = fix_date.split(".")[0].strip()
            fixture_info["time"] = row.xpath("td[3]/text()").get().strip()
            fixture_info["venue"] = row.xpath("td[4]/text()").get().strip()
            fixture_info["matchday_rank"] = (
                None
                if row.xpath("td[5]/span/text()").get() is None
                else row.xpath("td[5]/span/text()").get().strip()
            )
            fixture_info["opponent_team"] = row.xpath("td[7]/a/@title").get()
            fixture_info["opponent_matchday_rank"] = (
                None
                if row.xpath("td[7]/span/text()").get() is None
                else row.xpath("td[7]/span/text()").get().strip()
            )
            fixture_info["goals_scored"] = (
                row.xpath("td[10]/a/span/text()").get().strip().split(":")[0]
                if fixture_info["venue"].lower() == "h"
                else row.xpath("td[10]/a/span/text()").get().strip().split(":")[1]
            )
            fixture_info["goals_conceded"] = (
                row.xpath("td[10]/a/span/text()").get().strip().split(":")[1]
                if fixture_info["venue"].lower() == "h"
                else row.xpath("td[10]/a/span/text()").get().strip().split(":")[0]
            )
            self.logger.debug(f"FIXTURE_INFO: {fixture_info}")
