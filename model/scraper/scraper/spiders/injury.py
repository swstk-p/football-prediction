from .base import BaseSpider, scrapy


class InjurySpider(BaseSpider):
    name = "injury"

    def __init__(self):
        super().__init__()
        self.injuries = self.get_injury_obj()

    def start_requests(self):
        self.injuries.parse_missing_injuries_played()
