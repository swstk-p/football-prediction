import subprocess


def run_spiders():
    """Runs command line scripts for scraping"""
    subprocess.run(["scrapy", "crawl", "country_code"])
    subprocess.run(["scrapy", "crawl", "comp_name"])
    subprocess.run(["scrapy", "crawl", "club_name"])
    subprocess.run(["scrapy", "crawl", "fixture"])
    subprocess.run(["scrapy", "crawl", "injury"])


if __name__ == "__main__":
    run_spiders()
