from .base import BaseSpider, scrapy


class InjurySpider(BaseSpider):
    def __init__(self):
        super.__init__()
        self.injuries = self.get_injury_obj()
