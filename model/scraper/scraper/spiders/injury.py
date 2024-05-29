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
        # missing_api_team_id: list = self.injuries.get_missing_api_team_id()
        # headers = {
        #     "x-rapidapi-host": "v3.football.api-sports.io",
        #     "x-apisports-key": os.getenv("INJURY_API_KEYS"),
        # }
        # parsing API team_id for teams
        # for team in missing_api_team_id:
        #     yield scrapy.Request(
        #         url=f"https://v3.football.api-sports.io/teams?name={urllib.parse.quote(team.split()[0])}",
        #         headers=headers,
        #         callback=self.parse_api_team_id,
        #     )

        # retrieving dasboard pages for team id
        # headers = {
        #     ":authority": "dashboard.api-football.com",
        #     ":method":"GET",
        #     ":path": "/soccer/ids/teams/England",
        #     ":scheme":"https",

        #     "Cache-Control": "max-age=0",
        #     "Cookie": "PHPSESSID=2afb714e618da2daa373b0922465ca66; sess_active=%7B%2267748%22%3A1716779034%7D; G_ENABLED_IDPS=google; 893ed3a2afdaa40038c30de64eb184c68c26f4ba=9edfdefd58ce534d943f9ccf62a5f8f6",
        #     "Priority": "u=0, i",
        #     "Referer": "https://dashboard.api-football.com/soccer/ids/teams/England",
        #     "Sec-Ch-Ua": '"Brave";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        #     "Sec-Ch-Ua-Mobile:": "?1",
        #     "Sec-Ch-Ua-Platform": '"Android"',
        #     "Sec-Fetch-Dest": "document",
        #     "Sec-Fetch-Mode": "navigate",
        #     "Sec-Fetch-Site": "same-origin",
        #     "Sec-Gpc": "1",
        #     "Upgrade-Insecure-Requests": "1",
        #     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
        # }
        # yield scrapy.Request(
        #     url="https://dashboard.api-football.com/soccer/ids/teams/England",
        #     headers=headers,
        #     callback=self.parse_api_team_id,
        # )

        # requesting login page
        # yield scrapy.Request(
        #     url="https://dashboard.api-football.com/login", callback=self.login_to_page
        # )

        # parsing team_id using selenium
        self.injuries.parse_team_id()

    

    def parse_api_team_id(self, response):
        # data = json.loads(response.body)
        self.injuries.logger.debug(f"RESPONSE_DATA: {response}")
