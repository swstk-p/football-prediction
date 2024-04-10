from .base import BaseSpider, scrapy


class CompNameSpider(BaseSpider):
    name = "comp_name"

    def __init__(self):
        super().__init__()

    def start_requests(self):
        """Checks if all the competitions are available; if not then generates urls from country codes and sends requests

        Yields:
            _type_: scrapy.Request
        """
        have_all_domestic_comps: bool = self.comp_names.have_all_domestic_comps()
        if not have_all_domestic_comps:
            # get urls for each country pages
            all_country_urls = self.country_codes.get_all_country_urls()
            # follow each country urls
            for country, url in all_country_urls.items():
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_domestic_comp,
                    cb_kwargs={"country": country},
                )
        # TODO: scrape European competitions as well

    def parse_domestic_comp(self, response, country):
        """Parses the competition names from the response and writes them

        Args:
            response (_type_): response object from spider
            country (_type_): country name
        """
        comps = self.comp_names.parse_domestic_comp_names(response, country)
        self.comp_names.write_to_json_file(comps)
