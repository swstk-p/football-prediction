from .base import BaseSpider, scrapy
from pprint import pp


class CompNameSpider(BaseSpider):
    name = "comp_name"

    def __init__(self):
        super().__init__()
        self.comp_names = self.get_comp_name_obj()

    def start_requests(self):
        """Checks if all the competitions are available; if not then generates urls from country codes and sends requests

        Yields:
            _type_: scrapy.Request
        """
        # parsing domestic comps
        have_all_domestic_comps: bool = self.comp_names.have_all_domestic_comps()
        if not have_all_domestic_comps:
            # get urls for each country pages
            all_country_urls = self.comp_names.get_all_country_urls()
            # follow each country urls
            for country, url in all_country_urls.items():
                self.comp_names.logger.info(
                    f"Scraping {country}'s url to parse domestic competition names."
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_domestic_comp,
                    cb_kwargs={"country": country},
                )
        else:
            self.comp_names.logger.info(
                "Domestic competition names not scraped because they were found in the database."
            )
            # dont update current seasons if parsing all competition names; update current season if all comps names already parsed
            all_country_urls = self.comp_names.get_all_country_urls()
            for country, url in all_country_urls.items():
                self.comp_names.logger.info(
                    f"Scraping {country}'s url to update its current season."
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.update_current_season,
                    cb_kwargs={"country": country},
                )
        # parsing intl comps
        have_all_intl_comps: bool = self.comp_names.have_all_intl_comps()
        if not have_all_intl_comps:
            self.comp_names.logger.info("Scraping url to parse intl competition names.")
            yield scrapy.Request(
                url="https://www.transfermarkt.com/wettbewerbe/europa",
                callback=self.parse_intl_comp,
            )
        else:
            self.comp_names.logger.info(
                "Intl competition names not scraped because they were found in the database."
            )

    def parse_domestic_comp(self, response, country):
        """Parses the competition names from the response and writes them

        Args:
            response (_type_): response object from spider
            country (_type_): country name
        """
        comps = self.comp_names.parse_domestic_comp_names(response, country)
        self.comp_names.record_in_db(comps)

    def parse_intl_comp(self, response):
        """Parses the intl (UEFA) competition names from the response and writes them

        Args:
            response (_type_): response object from spider
        """
        comps = self.comp_names.parse_intl_comp_names(response)
        self.comp_names.record_in_db(comps)

    def update_current_season(self, response, country: str):
        """Updates the current season for the country and records in the database

        Args:
            response (_type_): response object from the spider
            country (_type_): country name in string
        """
        self.comp_names.update_current_seasons_in_db(response, country)
