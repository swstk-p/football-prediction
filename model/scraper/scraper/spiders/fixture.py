from .base import BaseSpider, scrapy


class FixtureSpider(BaseSpider):
    name = "fixture"

    def __init__(self):
        super().__init__()
        self.fixtures = self.get_fixture_obj()

    def start_requests(self):
        all_fixture_urls = self.fixtures.get_all_club_all_season_fixture_urls()
        for league, fixtures in all_fixture_urls.items():
            self.fixtures.logger.info(f"Scraping fixture urls for {league}.")
            for fixture in fixtures:
                yield scrapy.Request(
                    url=fixture["url"],
                    callback=self.parse,
                    cb_kwargs={"team": fixture["team"], "season": fixture["season"]},
                )

    def parse(self, response, team, season):
        fixture_info = self.fixtures.parse_all_fixtures_info(response, team, season)
        self.fixtures.record_fixtures_in_db(fixture_info)
