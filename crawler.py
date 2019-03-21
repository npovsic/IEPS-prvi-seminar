from multiprocessing import Process
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime

from robotparser import RobotFileParser
from database_handler import DatabaseHandler

# Create a global database handler for all processes
database_handler = DatabaseHandler(0, 100)

# https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Complete_list_of_MIME_types
CONTENT_TYPES = {
    "html": "text/html",
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "ppt": "application/vnd.ms-powerpoint",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "a": "a",
}

PAGE_TYPES = {
    "html": "HTML",
    "binary": "BINARY",
    "duplicate": "DUPLICATE",
    "frontier": "FRONTIER"
}


class Crawler:
    def __init__(self, number_of_processes):
        print("Created crawler")

        self.number_of_processes = number_of_processes

        with open("seed_pages.txt", "r") as seed_pages:
            for seed_page in seed_pages:
                if "#" not in seed_page:
                    database_handler.insert_seed_page(seed_page.strip())

        # When starting the crawler reset frontier active flags
        database_handler.reset_frontier()

    def run(self):
        for i in range(self.number_of_processes):
            p = Process(target=self.create_process)
            p.start()

    def create_process(self):
        # All the processes share the database handler (because of locking)
        crawler_process = CrawlerProcess()


class CrawlerProcess:
    def __init__(self):
        print("Created crawler process")

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("headless")
        self.driver = webdriver.Chrome(chrome_options=chrome_options)

        self.database_handler = database_handler

        """
            frontier page is a dictionary with an id (database id for updating) and url fields
        """
        self.current_page = None
        self.page_data = None
        self.site = None

        self.robots_parser = None
        self.sitemaps = []

        self.pages_to_add_to_frontier = []

        self.is_site_new = False

        self.run()

    def run(self):
        self.current_page = self.get_page_from_frontier()
        self.page_data = None
        self.site = None

        self.robots_parser = None
        self.sitemaps = []

        self.pages_to_add_to_frontier = []

        self.is_site_new = False

        # TODO: this is not a sufficient condition, because the frontier may yet be populated by another process
        if self.current_page is None:
            print("No page in frontier")

            return

        print("CURRENT URL", self.current_page["url"])

        domain = self.get_domain_url(self.current_page["url"])

        self.site = database_handler.get_site(domain)

        if self.site is None:
            # We need to create a new site object

            self.is_site_new = True

            robots = self.fetch_robots(domain)

            sitemap = None

            if robots is not None:
                self.parse_robots(robots)

            if len(self.sitemaps) > 0:
                for sitemap_url in self.sitemaps:
                    response = self.fetch_response(sitemap_url)

                    if response.status_code is 200:
                        sitemap = response.text

                        self.parse_sitemap(response.text)

            self.site = {
                "domain": domain,
                "robots_content": robots,
                "sitemap_content": sitemap
            }

            self.site["id"] = database_handler.insert_site(self.site)

        if (self.site["robots_content"] is not None) and (self.robots_parser is None):
            self.parse_robots(self.site["robots_content"])

        self.current_page["site_id"] = self.site["id"]

        self.current_page["accessed_time"] = datetime.now()

        # Fetch a head response so that we can save the http code
        page_response = self.fetch_response(self.current_page["url"])

        self.current_page["http_status_code"] = page_response.status_code

        if CONTENT_TYPES["html"] in page_response.headers['content-type']:
            # We got an HTML page

            self.current_page["page_type_code"] = PAGE_TYPES["html"]

            """
                If a page is of type HTML, its content should be stored as a value within html_content attribute
            """

            self.current_page["html_content"] = self.fetch_rendered_page_source(self.current_page["url"])

            # 4. check for duplicate html data (use a hash algorithm that return a similar value for similar pages)
            """
                The duplicate page should not have the html_content value set, page_type_code should be DUPLICATE and
                 that's it
            """

            self.parse_page(self.current_page["html_content"])

            # TODO: update link table with from and to values
        else:
            print("Received something other than HTML", page_response.headers['content-type'])

            """
                if crawler detects a binary file (e.g. .doc) html_content is set to NULL and a record in the page_data 
                table is created
            """

        # Update the page in the database, remove FRONTIER type and replace it with the correct one
        database_handler.update_page(self.current_page)

        print("CURRENT PAGE", self.current_page)
        print("CURRENT SITE", self.site)

        print("\n")
        print("\n")

        # Run the crawler again with a new page from the frontier
        self.run()

    def get_page_from_frontier(self):
        return self.database_handler.get_page_from_frontier()

    def fetch_response(self, url):
        response = requests.get(url)

        return response

    def fetch_head(self, url):
        response = requests.head(url)

        return response

    def fetch_rendered_page_source(self, url):
        self.driver.get(url)

        return self.driver.page_source

    def get_domain_url(self, url):
        parsed_uri = urlparse(url)

        return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    def fetch_robots(self, domain):
        response = self.fetch_response(domain + "/robots.txt")

        if response.status_code is 200:
            # Robots found
            return response.text

        return None

    """
        This function parsers the robots from memory and populates the self.sitemaps array if the site is new and 
        sitemaps are available
    """
    def parse_robots(self, robots_text):
        self.robots_parser = RobotFileParser(robots_text)
        self.robots_parser.read()

        if self.is_site_new:
            self.sitemaps = self.robots_parser.get_sitemaps()

    def parse_sitemap(self, sitemap_text):
        print("SITEMAP TEXT", sitemap_text)

    def parse_page(self, html_content):
        print("Parse page with BeautifulSoup")

        soup = BeautifulSoup(html_content, 'html.parser')

        """
            When parsing links, include links from href attributes and onclick Javascript events (e.g. location.href or 
            document.location). Be careful to correctly extend the relative URLs before adding them to the frontier.
        """

        """
            Detect images on a web page only based on img tag, where the src attribute points to an image URL.
        """

    def add_page_to_frontier_array(self, page_url):
        print("ADD TO ARRAY", page_url)
        self.pages_to_add_to_frontier.append(page_url)

    def add_pages_to_frontier(self):
        database_handler.add_pages_to_frontier(self.pages_to_add_to_frontier)

    def quit(self):
        self.driver.quit()
