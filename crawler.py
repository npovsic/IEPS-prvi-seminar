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
    "HTML": "text/html",
    "PDF": "application/pdf",
    "DOC": "application/msword",
    "DOCX": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "PPT": "application/vnd.ms-powerpoint",
    "PPTX": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "A": "a"
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
        self.site = None

        self.robots_parser = None

        self.pages_to_add_to_frontier = []

        self.run()

    def run(self):
        self.current_page = self.get_page_from_frontier()
        self.site = None

        self.robots_parser = None

        self.pages_to_add_to_frontier = []

        # TODO: this is not a sufficient condition, because the frontier may yet be populated by another process
        if self.current_page is None:
            print("No page in frontier")

            return

        print("CURRENT URL", self.current_page["url"])

        domain = self.get_domain_url(self.current_page["url"])

        self.site = database_handler.get_site(domain)

        if self.site is None:
            # We need to create a new site object

            robots = self.fetch_robots(domain)

            sitemap = None

            if robots is not None:
                self.parse_robots(robots)

                sitemaps = self.robots_parser.get_sitemaps()

                if len(sitemaps) > 0:
                    for sitemap_url in sitemaps:
                        sitemap = self.fetch_sitemap(sitemap_url)

                        if sitemap is not None:
                            self.parse_sitemap(sitemap)

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

        content_type = page_response.headers['content-type']

        self.current_page["http_status_code"] = page_response.status_code

        if CONTENT_TYPES["HTML"] in content_type:
            # We got an HTML page

            self.current_page["page_type_code"] = PAGE_TYPES["html"]

            self.current_page["html_content"] = self.fetch_rendered_page_source(self.current_page["url"])

            self.check_for_duplication(self.current_page["html_content"])

            self.parse_page(self.current_page["html_content"])
        else:
            # The crawler detected a binary file so a new record in the paga_data table is created

            self.current_page["page_type_code"] = PAGE_TYPES["binary"]

            self.current_page["html_content"] = None

            data_type_code = None

            for code, value in CONTENT_TYPES.items():
                if content_type == value:
                    data_type_code = code

            page_data = {
                "page_id": self.current_page["id"],
                "data_type_code": data_type_code,
                "data": page_response.content
            }

            database_handler.insert_page_data(page_data)

        # Update the page in the database, remove FRONTIER type and replace it with the correct one
        database_handler.update_page(self.current_page)

        print("CURRENT PAGE", self.current_page)
        print("CURRENT SITE", self.site)

        print("\n")
        print("\n")

        self.add_pages_to_frontier()

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

    def fetch_sitemap(self, sitemap_url):
        response = self.fetch_response(sitemap_url)

        if response.status_code is 200:
            # Robots found
            return response.text

        return None

    """
        This function parses the robots.txt from memory
    """
    def parse_robots(self, robots_text):
        self.robots_parser = RobotFileParser(robots_text)
        self.robots_parser.read()

    """
        https://stackoverflow.com/questions/31276001/parse-xml-sitemap-with-python
        
        This only works for the standard XML sitemap
    """
    def parse_sitemap(self, sitemap_xml):
        soup = BeautifulSoup(sitemap_xml, 'lxml')

        sitemap_tags = soup.find_all("loc")

        for sitemap_tag in sitemap_tags:
            url = sitemap_tag.text

            self.add_page_to_frontier_array(url)

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

    """
        TODO: use a hash algorithm that return a similar value for similar pages
        The duplicate page should not have the html_content value set, page_type_code should be DUPLICATE and
         that's it
    """
    def check_for_duplication(self, html_content):
        print("Check duplicated")

    def add_page_to_frontier_array(self, page_url):
        self.pages_to_add_to_frontier.append(page_url)

    def add_pages_to_frontier(self):
        database_handler.add_pages_to_frontier(self.pages_to_add_to_frontier)

    def quit(self):
        self.driver.quit()
