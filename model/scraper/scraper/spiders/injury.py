from .base import BaseSpider, scrapy


class InjurySpider(BaseSpider):
    name = "injury"

    def __init__(self):
        super().__init__()
        self.injuries = self.get_injury_obj()

    def start_requests(self):
        missing_api_team_id: list = self.injuries.get_missing_api_team_id()
        for team in missing_api_team_id:
            yield scrapy.Request(url=f"v3.football.api-sports.io/injuries?")
