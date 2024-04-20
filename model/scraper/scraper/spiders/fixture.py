from .base import BaseSpider, scrapy


class FixtureSpider(BaseSpider):
    name = "fixture"

    def __init__(self):
        super().__init__()
        self.fixtures = self.get_fixture_obj()

    def start_requests(self):
        all_fixture_urls = self.fixtures.get_all_club_all_season_fixture_urls()
