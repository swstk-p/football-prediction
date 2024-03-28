from pathlib import Path

import scrapy


class TransfermarktSpider(scrapy.Spider):
    def __init__(self, start_urls: list[str], name: str = None):
        self.name = "transfermarkt" if name is None else name
        self.start_urls = start_urls
