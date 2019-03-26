from multiprocessing import Process, current_process
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urlparse, quote
from datetime import datetime
from robotparser import RobotFileParser
from database_handler import DatabaseHandler
import re
import time
import hashlib

# Create a global database handler for all processes to share
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
    "IMG": "image"
}

PAGE_TYPES = {
    "html": "HTML",
    "binary": "BINARY",
    "image": "IMAGE",
    "duplicate": "DUPLICATE",
    "frontier": "FRONTIER",
    "error": "ERROR",
    "disallowed": "DISALLOWED"
}

# Only scrape sites in the gov.si domain
ALLOWED_DOMAIN = ".gov.si"

# The maximum number of retries for a crawler process if the frontier is empty
MAX_NUMBER_OF_RETRIES = 5

# Delay for retrying to fetch a page from the frontier
DELAY = 10


class Crawler:
    def __init__(self, number_of_processes):
        self.number_of_processes = number_of_processes

        with open("seed_pages.txt", "r") as seed_pages:
            for seed_page in seed_pages:
                if "#" not in seed_page:
                    database_handler.add_page_to_frontier(seed_page.strip())

        # When starting the crawler reset frontier active flags
        database_handler.reset_frontier()

    def run(self):
        for i in range(self.number_of_processes):
            p = Process(target=self.create_process, args=[i])
            p.start()

    def create_process(self, index):
        crawler_process = CrawlerProcess(index)


class CrawlerProcess:
    def __init__(self, index):
        self.current_process_id = index

        number_of_retries = 0

        print("[CREATED CRAWLER PROCESS]", self.current_process_id)

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
        self.current_page = database_handler.get_page_from_frontier()

        """
            If a page was fetched from the frontier the crawler can continue, otherwise try again in DELAY seconds
            
            If the frontier is still empty after MAX_NUMBER_OF_RETRIES was reached, we can assume that the frontier is
            really empty and no crawler process is going to insert new pages
        """
        while self.current_page or number_of_retries < MAX_NUMBER_OF_RETRIES:
            if self.current_page:
                number_of_retries = 0

                self.crawl()
            else:
                # No page was fetched from the frontier, try again in DELAY seconds
                number_of_retries += 1

                time.sleep(DELAY)

            # Reset all variables after a page was successfully transferred from the frontier

            self.current_page = database_handler.get_page_from_frontier()

            self.site = None

            self.robots_parser = None

            self.pages_to_add_to_frontier = []

            # TODO: check for spider traps (limit amount of pages from a single site, limit the length of an url)

        self.quit()

        print("[STOPPED CRAWLER PROCESS] Frontier is empty after several tries", self.current_process_id)

    def crawl(self):
        print(" {} - [CRAWLING PAGE]".format(self.current_process_id), self.current_page["url"])

        domain = self.get_domain_url(self.current_page["url"])

        self.site = database_handler.get_site(domain)

        if self.site is None:
            self.create_site(domain)
        else:
            if self.site["robots_content"] is not None:
                # Create robots_parser from robots.txt saved in the database
                self.parse_robots(self.site["robots_content"])

        self.current_page["site_id"] = self.site["id"]

        self.current_page["accessed_time"] = datetime.now()

        if self.allowed_to_crawl_current_page(self.current_page["url"]) is False:
            print("     [CRAWLING] Robots do not allow this site to be crawled")

            self.current_page["page_type_code"] = PAGE_TYPES["disallowed"]

            self.current_page["http_status_code"] = 500

            self.current_page["html_content"] = None

            database_handler.remove_page_from_frontier(self.current_page)

            return

        # The crawler is allowed to crawl the current site, therefore we can perform a request
        page_response = self.fetch_response(self.current_page["url"])

        if page_response:
            # No errors while fetching the response

            content_type = page_response.headers['content-type']

            self.current_page["http_status_code"] = page_response.status_code

            if CONTENT_TYPES["HTML"] in content_type:
                # We got an HTML page

                html_content = self.fetch_rendered_page_source(self.current_page["url"])

                if self.is_duplicate_page(html_content):
                    print("     [CRAWLING] Found page duplicate, that has already been parsed: ", self.current_page["url"])

                    self.current_page["page_type_code"] = PAGE_TYPES["duplicate"]

                    self.current_page["html_content"] = None

                    self.current_page["hash_content"] = None
                else:
                    self.current_page["page_type_code"] = PAGE_TYPES["html"]

                    self.current_page["html_content"] = html_content

                    self.current_page["hash_content"] = self.create_content_hash(html_content)

                    parsed_page = self.parse_page(self.current_page["html_content"])

                    if len(parsed_page['links']):
                        for link in parsed_page['links']:

                            self.add_page_to_frontier_array(link)

                    if len(parsed_page['images']):
                        for image_url in parsed_page['images']:
                            self.add_page_to_frontier_array(image_url)

            elif CONTENT_TYPES["IMG"] in content_type:
                # We can be pretty sure that we have an image

                self.current_page["page_type_code"] = PAGE_TYPES["image"]

                self.current_page["html_content"] = None

                filename = self.get_image_filename(self.current_page["url"])

                image_data = {
                    "page_id": self.current_page["id"],
                    "content_type": content_type,
                    "data": page_response.content,
                    "accessed_time": datetime.now(),
                    "filename": filename
                }

                database_handler.insert_image_data(image_data)

            else:
                # The crawler detected a non-image binary file

                self.current_page["page_type_code"] = PAGE_TYPES["binary"]

                self.current_page["html_content"] = None

                data_type_code = None

                # Find the correct data_type_code from all the content types
                for code, value in CONTENT_TYPES.items():
                    if content_type == value:
                        data_type_code = code

                if data_type_code is None:
                    # The content type is not in the allowed values, therefore we can ignore it

                    print("     [CRAWLING] Page response content-type is not in CONTENT_TYPES: ", content_type)
                else:
                    page_data = {
                        "page_id": self.current_page["id"],
                        "data_type_code": data_type_code,
                        "data": page_response.content
                    }

                    database_handler.insert_page_data(page_data)

        else:
            # An error occurred while fetching page (SSL certificate error, timeout, etc.)

            self.current_page["page_type_code"] = PAGE_TYPES["error"]

            self.current_page["http_status_code"] = 500

            self.current_page["html_content"] = None

        # Update the page in the database, remove FRONTIER type and replace it with the correct one
        database_handler.remove_page_from_frontier(self.current_page)

        # Add all the links from the page and sitemap to the frontier
        database_handler.add_pages_to_frontier(self.pages_to_add_to_frontier)

        print("     [CRAWLING] Finished crawling")

    """
        Fetch a response from the url, so that we get the status code and find out if any errors occur while fetching
        (some sites for example require a certificate to connect, some sites timeout, etc.)
    """
    def fetch_response(self, url):
        try:
            response = requests.get(url)

            return response
        except requests.exceptions.RequestException as exception:
            print("     [CRAWLING - ERROR]", exception)

            return None

    """
        Create a new site object and insert it into the database
    """
    def create_site(self, domain):
        # We need to create a new site object

        self.site = {
            "domain": domain
        }

        robots_content = self.fetch_robots(domain)

        sitemap_content = None

        if robots_content is not None:
            # Create robots_parser from fetched robots.txt
            self.parse_robots(robots_content)

            sitemaps = self.robots_parser.get_sitemaps()

            if len(sitemaps) > 0:
                for sitemap_url in sitemaps:
                    sitemap_content = self.fetch_sitemap(sitemap_url)

                    if sitemap_content is not None:
                        self.parse_sitemap(sitemap_content)

        self.site["robots_content"] = robots_content
        self.site["sitemap_content"] = sitemap_content

        # Insert the new site into database and return the id
        self.site["id"] = database_handler.insert_site(self.site)

    """
        Fetch and render the site in the chrome driver then return the resulting html so that it can be saved in the 
        current page html_content
    """
    def fetch_rendered_page_source(self, url):
        self.driver.get(url)

        return self.driver.page_source

    """
        Get the domain name of the current site so that we can check if the site is already in the database or if we
        have to create it
    """
    def get_domain_url(self, url):
        parsed_uri = urlparse(url)

        return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    """
        Get the filename from an online image resource
        https://stackoverflow.com/questions/10552188/python-split-url-to-find-image-name-and-extension
    """
    def get_image_filename(self, image_url):
        filename = image_url.split('/')[-1]

        return filename

    def fetch_robots(self, domain):
        response = self.fetch_response(domain + "/robots.txt")

        # We need to check if the returned file is actually a txt file, because some sites route back to the index page
        if response and response.status_code is 200 and "text/plain" in response.headers['content-type']:
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

        if sitemap_tags is None:
            return

        for sitemap_tag in sitemap_tags:
            url = self.get_parsed_url(sitemap_tag.text)

            if url:
                self.add_page_to_frontier_array(url)

    """
        Checks if robots are set for the current site and if they allow the crawling of the current page
    """
    def allowed_to_crawl_current_page(self, url):
        if self.robots_parser is not None:
            return self.robots_parser.can_fetch('*', url)

        return True

    """
        Use the chrome driver to fetch all links and image sources in the rendered page (the driver already returns 
        absolute urls)
    """
    def parse_page(self, html_content):
        browser = self.driver

        links = []
        images = []

        anchor_tags = browser.find_elements_by_tag_name("a")

        for anchor_tag in anchor_tags:
            href = anchor_tag.get_attribute("href")

            url = self.get_parsed_url(href)

            if url:
                links.append(url)

        image_tags = browser.find_elements_by_tag_name("img")

        for image_tag in image_tags:
            src = image_tag.get_attribute("src")

            if src:
                image_url = self.get_parsed_image_url(src)

                if image_url:
                    images.append(image_url)

        soup = BeautifulSoup(html_content, 'html.parser')

        script_tags = soup.findAll('script')

        for script_tag in script_tags:
            links_from_javascript = self.parse_links_from_javacript(script_tag.text)

            for link in links_from_javascript:
                links.append(link)

        return {
            "links": links,
            "images": images
        }

    """
        Find all the hrefs that are set in javascript code (window.location changes)
    """
    def parse_links_from_javacript(self, javascript_text):
        links = re.findall(r'(http://|https://)([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?',
                           javascript_text)

        if not links:
            return []

        links = [''.join(link) for link in links]

        return links

    """
        Create a parsed url (ignore javascript and html actions, remove hashes, fix relative urls etc.)
    """
    def get_parsed_url(self, url):
        if url is None or url is "":
            return None

        domain = self.site["domain"]

        if 'http' not in url:
            # Since the chrome driver returns absolute urls, the url is most likely javascript or action

            if 'javascript:' in url:
                # This is just javascript code inside a href
                return None

            if ('mailto:' in url) or ('tel:' in url):
                # This is an action inside a href
                return None

            if url[0] is "#":
                # Link starts with a # (it's a target link)
                return None

            if url is "/":
                # This is the index page, which we already have in the frontier
                return None

            """
                Fix relative urls just in case
                
                This function might not work correctly since it's almost impossible to know which root url the link 
                takes when it's added to the site
            """
            if url[0] == "/":
                if domain[-1] == "/":
                    # Make sure only one slash is present
                    url = url[1:]
            else:
                if domain[-1] != "/":
                    url = "/{}".format(url)

            # TODO: read up on anchor tags and how they determine the root domain for relative tags
            url = "{}{}".format(domain, url).strip()

        # Remove everything after the hash
        if "#" in url:
            url = url.split("#")[0]

        # Encode special characters (the second parameter are characters that the encoder will not encode)
        url = quote(url.encode("UTF-8"), ':/-_.~&?+=')

        return url

    """
        Parse image urls
    """
    def get_parsed_image_url(self, url):
        if url is None or url is "":
            return None

        # Do not parse base64 images
        if "data:image" in url:
            return None

        if 'http' not in url:
            # This is very unlikely, since the chrome driver returns all the image sources with absolute urls

            domain = self.site["domain"]

            """
                Fix relative urls just in case

                This function might not work correctly since it's almost impossible to know which root url the link 
                takes when it's added to the site
            """
            if url[0] == "/":
                if domain[-1] == "/":
                    # Make sure only one slash is present
                    url = url[1:]
            else:
                if domain[-1] != "/":
                    url = "/{}".format(url)

            # Create an absolute url
            url = "{}{}".format(domain, url).strip()

        return url

    def create_content_hash(self, html_content):
        m = hashlib.sha256()

        m.update(html_content.encode('utf-8'))

        return m.hexdigest()

    """
        TODO: use a hash algorithm that return a similar value for similar pages
        The duplicate page should not have the html_content value set, page_type_code should be DUPLICATE and
         that's it
    """
    # TODO: check for duplicates
    def is_duplicate_page(self, html_content):
        h = self.create_content_hash(html_content)

        return database_handler.find_page_duplicate(h)

    def add_page_to_frontier_array(self, page_url):
        if ALLOWED_DOMAIN in page_url:
            # Only add pages in the allowed domain

            self.pages_to_add_to_frontier.append({
                "from": self.current_page["id"],
                "to": page_url
            })

    def quit(self):
        self.driver.quit()
