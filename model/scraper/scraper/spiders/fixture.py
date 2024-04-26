from .base import BaseSpider, scrapy


class FixtureSpider(BaseSpider):
    name = "fixture"

    def __init__(self):
        super().__init__()
        self.fixtures = self.get_fixture_obj()

    def start_requests(self):
        # getting all the fixtures for all the seasons if not available
        have_all_fixtures_parsed: bool = self.fixtures.have_all_fixtures()
        if not have_all_fixtures_parsed:
            all_fixture_urls = self.fixtures.get_all_club_all_season_fixture_urls()
            for league, fixtures in all_fixture_urls.items():
                self.fixtures.logger.info(f"Scraping fixture urls for {league}.")
                for fixture in fixtures:
                    yield scrapy.Request(
                        url=fixture["url"],
                        callback=self.parse,
                        cb_kwargs={
                            "team": fixture["team"],
                            "season": fixture["season"],
                        },
                    )
        else:
            self.fixtures.logger.info(
                "Not scraped all the previous fixtures because previous fixtures are stored in the database."
            )
            # resetting all current season fixtures
            self.fixtures.reset_current_season_fixtures()
            # getting all the fixtures of only the current season to update played and upcoming fixtures
            current_fixture_urls = self.fixtures.get_all_club_current_season_urls()
            for league, fixtures in current_fixture_urls.items():
                self.fixtures.logger.info(
                    f"Scraping fixture urls for {league} to update upcoming fixtures."
                )
                for fixture in fixtures:
                    yield scrapy.Request(
                        url=fixture["url"],
                        callback=self.parse,
                        cb_kwargs={
                            "team": fixture["team"],
                            "season": fixture["season"],
                        },
                    )

    def parse(self, response, team, season):
        fixture_info = self.fixtures.parse_all_fixtures_info(response, team, season)
        self.fixtures.record_fixtures_in_db(fixture_info)
