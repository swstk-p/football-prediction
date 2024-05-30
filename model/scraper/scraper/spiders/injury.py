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
        # parsing team_id using selenium
        self.injuries.parse_team_id()

    def parse_api_team_id(self, response):
        # data = json.loads(response.body)
        self.injuries.logger.debug(f"RESPONSE_DATA: {response}")
