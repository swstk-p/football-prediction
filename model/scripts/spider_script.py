import subprocess


def run_spiders():
    """Runs command line scripts for scraping"""
    subprocess.run(["scrapy", "crawl", "country_code"])
    subprocess.run(["scrapy", "crawl", "comp_name"])
    subprocess.run(["scrapy", "crawl", "club_name"])


if __name__ == "__main__":
    run_spiders()
