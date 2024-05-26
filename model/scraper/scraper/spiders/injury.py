from .base import BaseSpider, scrapy
import urllib, json, os
from dotenv import load_dotenv


class InjurySpider(BaseSpider):
    load_dotenv()
    name = "injury"

    def __init__(self):
        super().__init__()
        self.injuries = self.get_injury_obj()

    def start_requests(self):
        # obtaining teams with missing API team_id
        missing_api_team_id: list = self.injuries.get_missing_api_team_id()
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-apisports-key": os.getenv("INJURY_API_KEYS"),
        }
        self.injuries.logger.debug(f"API_KEY: {headers['x-apisports-key']}")
        # parsing API team_id for teams
        for team in missing_api_team_id:
            yield scrapy.Request(
                url=f"https://v3.football.api-sports.io/teams?name={urllib.parse.quote(team.split()[0])}",
                headers=headers,
                callback=self.parse_api_team_id,
            )

    def parse_api_team_id(self, response):
        data = json.loads(response.body)
        self.injuries.logger.debug(f"RESPONSE_DATA: {data}")
