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
    "A": "a",
    "TXT": "text/plain"
}

PAGE_TYPES = {
    "html": "HTML",
    "binary": "BINARY",
    "duplicate": "DUPLICATE",
    "frontier": "FRONTIER",
    "error": "ERROR"
}

ALLOWED_DOMAIN = ".gov.si"


class Crawler:
    def __init__(self, number_of_processes):
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
        print("[CREATED CRAWLER PROCESS]")

        # Create the chrome driver with which we will fetch and parse sites
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("headless")
        self.driver = webdriver.Chrome(chrome_options=chrome_options)

        """
            site is a dictionary with all the fields from the database (domain, robots_content, sitemap_content)
        """
        self.site = None

        """
            robots_parser is an object which allows us to use the robots.txt file
        """
        self.robots_parser = None

        """
            Holds all the pages which will be added to the frontier at the end of each run
        """
        self.pages_to_add_to_frontier = []

        """
            current_page is a dictionary with an id (database id for updating) and url field
        """
        self.current_page = self.get_page_from_frontier()

        while self.current_page:
            self.run()

            self.current_page = self.get_page_from_frontier()

            self.site = None

            self.robots_parser = None

            self.pages_to_add_to_frontier = []

        # TODO: check for spider traps (limit amount of pages from a single site, limit the length of an url)

        # TODO: this is perhaps not a sufficient condition, because the frontier may yet be populated by another process
        if self.current_page is None:
            print("No page in frontier")

            return

    def run(self):
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

            # Insert the new site into database and return the id
            self.site["id"] = database_handler.insert_site(self.site)

        if (self.site["robots_content"] is not None) and (self.robots_parser is None):
            self.parse_robots(self.site["robots_content"])

        self.current_page["site_id"] = self.site["id"]

        self.current_page["accessed_time"] = datetime.now()

        if self.allowed_to_crawl_current_page(self.current_page["url"]) is False:
            print("ROBOTS DNO NTO ALLOW THIS SITE TO BE CRAWLED")

            return

        # Fetch a head response so that we can save the http code
        page_response = self.fetch_response(self.current_page["url"])

        print("PAGE RESPONSE", page_response)

        if page_response is not None:
            content_type = page_response.headers['content-type']

            self.current_page["http_status_code"] = page_response.status_code

            if CONTENT_TYPES["HTML"] in content_type:
                # We got an HTML page

                self.current_page["page_type_code"] = PAGE_TYPES["html"]

                self.current_page["html_content"] = self.fetch_rendered_page_source(self.current_page["url"])

                self.check_for_duplication(self.current_page["html_content"])

                parsed_page = self.parse_page(self.current_page["html_content"])

                if len(parsed_page['links']):
                    for link in parsed_page['links']:

                        if ALLOWED_DOMAIN in link:
                            self.add_page_to_frontier_array(link)

                if len(parsed_page['images']):
                    for image_url in parsed_page['images']:
                        self.add_page_to_frontier_array(image_url)
            else:
                # The crawler detected a binary file

                self.current_page["page_type_code"] = PAGE_TYPES["binary"]

                self.current_page["html_content"] = None

                data_type_code = None

                for code, value in CONTENT_TYPES.items():
                    if content_type == value:
                        data_type_code = code

                if data_type_code is None:
                    # There is a very good chance that we have an image

                    # TODO: check filename and extension to determine if it really is an image

                    filename = ""

                    image_data = {
                        "page_id": self.current_page["id"],
                        "content_type": content_type,
                        "data": page_response.content,
                        "accessed_time": datetime.now(),
                        "filename": filename
                    }

                    self.insert_image_data(image_data)
                else:
                    page_data = {
                        "page_id": self.current_page["id"],
                        "data_type_code": data_type_code,
                        "data": page_response.content
                    }

                    self.insert_page_data(page_data)
        else:
            self.current_page["page_type_code"] = PAGE_TYPES["error"]

            self.current_page["http_status_code"] = 500

            self.current_page["html_content"] = None

        # Update the page in the database, remove FRONTIER type and replace it with the correct one
        database_handler.update_page(self.current_page)

        self.add_pages_to_frontier()

    def get_page_from_frontier(self):
        return database_handler.get_page_from_frontier()

    def fetch_response(self, url):
        response = None

        try:
            # some sites require a certificate to access, which throws an error

            response = requests.get(url)

            return response
        except requests.exceptions.RequestException as exception:
            print(exception)

            return None

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

        # We need to check fi the returned file is actually a txt file, because some sites route back to the index page
        if response and response.status_code is 200 and CONTENT_TYPES["TXT"] in response.headers['content-type']:
            # Robots.txt found
            return response.text

        return None

    def fetch_sitemap(self, sitemap_url):
        response = self.fetch_response(sitemap_url)

        if response and response.status_code is 200:
            # Sitemap found
            return response.text

        return None

    """
        This function parses the robots.txt from memory using the modified robotparser class
        The self.robots_parser includes functions to check if the parser is allowed to parse a certain site
    """
    def parse_robots(self, robots_text):
        self.robots_parser = RobotFileParser(robots_text)
        self.robots_parser.read()

    """
        https://stackoverflow.com/questions/31276001/parse-xml-sitemap-with-python
        
        This only works for the standard XML sitemap
    """
    # TODO: error handling
    def parse_sitemap(self, sitemap_xml):
        soup = BeautifulSoup(sitemap_xml, 'lxml')

        sitemap_tags = soup.find_all("loc")

        for sitemap_tag in sitemap_tags:
            url = self.get_parsed_url(sitemap_tag.text)

            if url:
                if ALLOWED_DOMAIN in url:
                    self.add_page_to_frontier_array(url)

    """
        Checks if robots are set and if they allow the crawling of the current site
    """
    # TODO: use the robots_parser functions
    def allowed_to_crawl_current_page(self, url):
        return True

    def parse_page(self, html_content):
        links = []
        images = []

        soup = BeautifulSoup(html_content, 'html.parser')

        anchor_tags = soup.findAll("a")

        for anchor_tag in anchor_tags:
            if anchor_tag.has_attr('href'):
                href = anchor_tag['href']
                
                url = self.get_parsed_url(href)

                if url: 
                    links.append(url)

        image_tags = soup.findAll("img")

        for image_tag in image_tags:
            if image_tag.has_attr('src'):
                image_url = self.get_parsed_url(image_tag['src'])

                if image_url:
                    images.append(image_url)

        script_tags = soup.findAll('script')

        for script_tag in script_tags:
            links_from_javascript = self.parse_links_from_javacript(script_tag.text)

            for link in links_from_javascript:
                links.append(link)

        return {
            "links": links,
            "images": images
        }

    # TODO: get hrefs from onclick Javascript events (e.g. location.href or document.location)
    def parse_links_from_javacript(self, javascript_text):
        links = []

        return links

    """
        TODO: 
            remove port number
            add trailing slash
            fix relative urls
                if an url starts with / then it should be appended to the domain url
                if an url does not start with an / then it should be appended to the current page url
            remove hashes
            decode characters
            encode disallowed characters
            lower-case urls
            handle actions(email, tel, etc.)
                hrefs can contain tel, email action and such (used mainly for mobile)
            handle all urls starting with javascript
                some hrefs include javascript code in them (looks like javascript:some_code();)
    """
    # TODO: parse url
    def get_parsed_url(self, url):
        if 'http' not in url:
            # URL is most likely relative
            print("URL IS NOT STANDARD", url)
            
            return None

        return url

    """
        TODO: use a hash algorithm that return a similar value for similar pages
        The duplicate page should not have the html_content value set, page_type_code should be DUPLICATE and
         that's it
    """
    # TODO: check for duplicates
    def check_for_duplication(self, html_content):
        print("Check duplicated")

    def add_page_to_frontier_array(self, page_url):
        self.pages_to_add_to_frontier.append(page_url)

    def insert_page_data(self, page_data):
        database_handler.insert_page_data(page_data)

    def insert_image_data(self, image_data):
        print("INSERT IMAGE")

    def add_pages_to_frontier(self):
        database_handler.add_pages_to_frontier(self.pages_to_add_to_frontier)

    def quit(self):
        self.driver.quit()