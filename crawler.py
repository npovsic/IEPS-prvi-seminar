from multiprocessing import Process
import requests
from selenium import webdriver
from bs4 import BeautifulSoup

from database_handler import DatabaseHandler


class Crawler:
    def __init__(self, number_of_processes):
        print("Created crawler")

        self.number_of_processes = number_of_processes

        self.database_handler = DatabaseHandler()

        with open("seed_pages.txt", "r") as seed_pages:
            for seed_page in seed_pages:
                self.database_handler.insert_seed_page(seed_page.strip())

        # When starting the crawler reset frontier active flags
        self.database_handler.reset_frontier()

    def run(self):
        for i in range(self.number_of_processes):
            p = Process(target=self.create_process)
            p.start()

    def create_process(self):
        # All the processes share the database handler (because of locking)
        crawler_process = CrawlerProcess(self.database_handler)


class CrawlerProcess:
    def __init__(self, database_handler):
        print("Created crawler process")

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("headless")
        self.driver = webdriver.Chrome(chrome_options=chrome_options)

        self.database_handler = database_handler

        """
            frontier page is a dictionary with an id (database id for updating) and url fields
        """
        self.frontier_page = None

        self.run()

    def run(self):
        print("Start crawler process")

        self.get_frontier_page()

        # TODO: this is not a sufficient condition, because the frontier may yet be populated by another process
        if self.frontier_page is None:
            return

        # 1. parse domain url

        # 2. check if site is in db
        #   2.1 if site is not in db fetch robots and sitemap
        #       2.1.1 parse and respect robots
        #       2.1.2 add all sitemap urls to frontier
        #       2.1.2 add site to database
        #   2.2 fetch the site id, so that it can be saved alongside the page data

        # 3. headless render the page
        self.fetch_rendered_page_source()

        # 4. check for duplicate html data (use a hash algorithm that return a similar value for similar pages)
        """
            The duplicate page should not have set the html_content value and should be linked to a duplicate version of 
            a page
        """

        # 5. save page data and update the page table element with frontier_page id (update page_type with either HTML, DUPLICATE or BINARY
        """
            If a page is of type HTML, its content should be stored as a value within html_content attribute, otherwise 
            (if crawler detects a binary file - e.g. .doc), html_content is set to NULL and a record in the page_data 
            table is created
        """

        # 6. parse page links and images

        # 7. add page links to frontier
        #   7.1 update link table with from and to values

        # 8. save images and binary data (doc, pdf, ...)

        # 9. run again
        self.run()

    def get_frontier_page(self):
        self.frontier_page = self.database_handler.get_frontier_page()

        print("Frontier page", self.frontier_page)

    def fetch_response(self, url):
        response = requests.get(url)

        print(response)

    def fetch_rendered_page_source(self):
        url = self.frontier_page["url"]
        self.driver.get(url)

        print(self.driver.page_source)

    def get_domain_url(self, url):
        print("Parse domain")

    def is_site_in_db(self):
        print("Check if site with domain url is in database")

    def parse_robots(self, url):
        print("Parse robots")

    def parse_sitemap(self, url):
        print("Parse sitemap")

    def parse_page(self, page_html):
        print("Parse page")

        soup = BeautifulSoup(page_html, 'html.parser')

        """
            When parsing links, include links from href attributes and onclick Javascript events (e.g. location.href or 
            document.location). Be careful to correctly extend the relative URLs before adding them to the frontier.
        """

        """
            Detect images on a web page only based on img tag, where the src attribute points to an image URL.
        """

    def quit(self):
        self.driver.quit()
