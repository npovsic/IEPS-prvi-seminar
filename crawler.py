from multiprocessing import Process, Lock
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urlparse, quote
from datetime import datetime, timedelta
from robotparser import RobotFileParser
from database_handler import DatabaseHandler
import re
import time
import hashlib
import binascii
from hash_driver import HashDriver

# Create a global database handler for all processes to share
database_handler = DatabaseHandler(0, 100)

# Create a global hash driver for creating page signatures
hash_driver = HashDriver()

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

# Upper similarity limit of two document [0,1]
MAX_SIMILARITY = 0.95

class Crawler:
    def __init__(self, number_of_processes):
        self.number_of_processes = number_of_processes

        self.lock = Lock()

        start = time.time()

        with open("seed_pages.txt", "r") as seed_pages:
            for seed_page in seed_pages:
                if "#" not in seed_page:
                    database_handler.add_seed_page_to_frontier(seed_page.strip())

        # When starting the crawler reset frontier active flags
        database_handler.reset_frontier()

    def run(self):
        for i in range(self.number_of_processes):
            p = Process(target=self.create_process, args=[i, self.lock])
            p.start()

    def create_process(self, index, lock):
        crawler_process = CrawlerProcess(index, lock)


class CrawlerProcess:
    def __init__(self, index, lock):
        self.current_process_id = index

        self.lock = lock

        number_of_retries = 0

        #print("[CREATED CRAWLER PROCESS]", self.current_process_id)

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
        self.current_page = database_handler.get_page_from_frontier(self.lock)

        """
            If a page was fetched from the frontier the crawler can continue, otherwise try again in DELAY seconds
            
            If the frontier is still empty after MAX_NUMBER_OF_RETRIES was reached, we can assume that the frontier is
            really empty and no crawler process is going to insert new pages
        """
        while self.current_page or number_of_retries < MAX_NUMBER_OF_RETRIES:
            if self.current_page:
                number_of_retries = 0

                try:
                    self.crawl()
                except Exception as error:
                    print("[CRAWLER PROCESS] An unhandled error occurred while parsing page: {}".format(
                        self.current_page["url"]), error)
            else:
                # No page was fetched from the frontier, try again in DELAY seconds
                number_of_retries += 1

                print("[CRAWLER PROCESS] Frontier is empty, retrying in 10 seconds", self.current_process_id)

                time.sleep(DELAY)

            # Reset all variables after a page was successfully transferred from the frontier

            self.current_page = database_handler.get_page_from_frontier(self.lock)

            self.site = None

            self.robots_parser = None

            self.pages_to_add_to_frontier = []

        self.quit()

        print("[STOPPED CRAWLER PROCESS] Frontier is empty after several tries", self.current_process_id)

    def crawl(self):
        #print(" {} - [CRAWLING PAGE]".format(self.current_process_id), self.current_page["url"])

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
            #print("     [CRAWLING] Robots do not allow this page to be crawled: {}".format(self.current_page["url"]))

            self.current_page["page_type_code"] = PAGE_TYPES["disallowed"]

            self.current_page["http_status_code"] = 500

            database_handler.remove_page_from_frontier(self.current_page)

            return
        else:
            # If a crawl delay is available in robots wait until the page can be crawled then continue
            self.wait_for_crawl_delay_to_elapse()

        # The crawler is allowed to crawl the current site, therefore we can perform a request
        page_response = self.fetch_response(self.current_page["url"])

        if page_response:
            # No errors while fetching the response

            content_type = ""

            if "content-type" in page_response.headers:
                # Content type is not necessarily always present (e. g. when Transfer-Encoding is set)
                content_type = page_response.headers['content-type']

            self.current_page["http_status_code"] = page_response.status_code

            if CONTENT_TYPES["HTML"] in content_type:
                # We got an HTML page

                html_content = self.fetch_rendered_page_source(self.current_page["url"])

                if html_content is not None:
                    if self.is_duplicate_page(html_content):
                        print("     [CRAWLING] Found page duplicate, that has already been parsed: ",
                              self.current_page["url"])

                        self.current_page["page_type_code"] = PAGE_TYPES["duplicate"]

                        self.current_page["hash_content"] = hash_driver.create_content_hash(html_content)

                    else:
                        # page is not treated as duplicate page - insert hash signature to db
                        database_handler.insert_page_signatures(self.current_page["id"],
                                                                self.current_page["hash_signature"])

                        self.current_page["page_type_code"] = PAGE_TYPES["html"]

                        self.current_page["html_content"] = html_content

                        self.current_page["hash_content"] = hash_driver.create_content_hash(html_content)

                        parsed_page = self.parse_page(self.current_page["html_content"])

                        if len(parsed_page['links']):
                            for link in parsed_page['links']:
                                self.add_page_to_frontier_array(link)

                        if len(parsed_page['images']):
                            for image_url in parsed_page['images']:
                                self.add_page_to_frontier_array(image_url)
                else:
                    # An error occurred while rendering page

                    self.current_page["page_type_code"] = PAGE_TYPES["error"]

                    self.current_page["http_status_code"] = 500

            elif CONTENT_TYPES["IMG"] in content_type:
                # We can be pretty sure that we have an image

                self.current_page["page_type_code"] = PAGE_TYPES["image"]

                filename = self.get_image_filename(self.current_page["url"])

                image_data = {
                    "page_id": self.current_page["id"],
                    "content_type": content_type,
                    "data": page_response.content,
                    "data_size": len(page_response.content),
                    "accessed_time": datetime.now(),
                    "filename": filename
                }

                database_handler.insert_image_data(image_data)

            else:
                # The crawler detected a non-image binary file

                self.current_page["page_type_code"] = PAGE_TYPES["binary"]

                data_type_code = None

                # Find the correct data_type_code from all the content types
                for code, value in CONTENT_TYPES.items():
                    if content_type == value:
                        data_type_code = code

                if data_type_code is None:
                    # The content type is not in the allowed values, therefore we can ignore it
                    testing = None
                    #print("     [CRAWLING] Page response content-type is not in CONTENT_TYPES: ", content_type)
                else:
                    page_data = {
                        "page_id": self.current_page["id"],
                        "data_type_code": data_type_code,
                        "data": page_response.content,
                        "data_size": len(page_response.content)
                    }

                    database_handler.insert_page_data(page_data)

        else:
            # An error occurred while fetching page (SSL certificate error, timeout, etc.)

            self.current_page["page_type_code"] = PAGE_TYPES["error"]

            self.current_page["http_status_code"] = 500

        # Update the page in the database, remove FRONTIER type and replace it with the correct one
        database_handler.remove_page_from_frontier(self.current_page)

        # Add all the links from the page and sitemap to the frontier
        database_handler.add_pages_to_frontier(self.pages_to_add_to_frontier)

        #print(" {} - [CRAWLING] Finished crawling".format(self.current_process_id))

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
        try:
            self.driver.get(url)

            return self.driver.page_source
        except Exception as error:
            print("     [CRAWLING] Error while fetching rendered page source", error)

            return None

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

    def parse_sitemap(self, sitemap_xml):
        try:
            soup = BeautifulSoup(sitemap_xml, 'lxml')

            sitemap_tags = soup.find_all("loc")

            if sitemap_tags is None:
                return

            for sitemap_tag in sitemap_tags:
                url = self.get_parsed_url(sitemap_tag.text)

                if url:
                    self.add_page_to_frontier_array(url)
        except Exception as error:
            print(error)

    """
        Checks if robots are set for the current site and if they allow the crawling of the current page
    """

    def allowed_to_crawl_current_page(self, url):
        if self.robots_parser is not None:
            return self.robots_parser.can_fetch('*', url)

        return True

    """
        Checks if crawl-delay property is set and if it exists check if the required time has elapsed
    """

    def wait_for_crawl_delay_to_elapse(self):
        try:
            if self.robots_parser is not None:
                crawl_delay = self.robots_parser.crawl_delay('*')

                if crawl_delay is not None:
                    if "last_crawled_at" in self.site and self.site["last_crawled_at"] is not None:
                        site_last_crawled_at = self.site["last_crawled_at"]

                        can_crawl_again_at = site_last_crawled_at + timedelta(seconds=crawl_delay)

                        current_time = datetime.now()

                        time_difference = (can_crawl_again_at - current_time).total_seconds()

                        if time_difference > 0:
                            #print("     [CRAWLING] Crawl delay has not yet elapsed for site: {}".format(
                            #   self.site["domain"]))

                            time.sleep(crawl_delay)
        except Exception as error:
            print("     [CRAWLING] Error while handling crawl delay", error)

    """
        Use the chrome driver to fetch all links and image sources in the rendered page (the driver already returns 
        absolute urls)
        
        Note: Sometimes throws StaleElementReferenceException, need to check what that's about. The exception itself 
        just means that the desired element is no longer rendered in DOM. Maybe the memory was getting low, since I got the
        error when I was running 10 crawler processes.
    """

    def parse_page(self, html_content):
        links = []
        images = []

        try:
            browser = self.driver

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
                    links.append(self.get_parsed_url(link))

            return {
                "links": links,
                "images": images
            }
        except Exception as error:
            print("[ERROR WHILE RENDERING WITH WEB DRIVER]", error)

            return {
                "links": links,
                "images": images
            }

    """
        Find all the hrefs that are set in javascript code (window.location changes)
    """

    def parse_links_from_javacript(self, javascript_text):
        links = []

        try:
            links = re.findall(r'(http://|https://)([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?',
                               javascript_text)

            if not links:
                return []

            links = [''.join(link) for link in links]
        except Exception as error:
            print("     [CRAWLING] Error while parsing links from Javascript", error)

        return links

    """
        Create a parsed url (ignore javascript and html actions, remove hashes, fix relative urls etc.)
    """

    # TODO: remove index.html index.php
    def get_parsed_url(self, url):
        if url is None or url is "":
            return None

        domain = self.site["domain"]

        if not url.startswith("http"):
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

            if url.startswith("www"):
                url = "http://{}".format(url).strip()

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
        if url.startswith("data:image"):
            return None

        if not url.startswith("http"):
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

    """
        The duplicate page should not have the html_content value set, page_type_code should be DUPLICATE and
         that's it
    """

    def is_duplicate_page(self, html_content):

        # sha256 digest of complete html_content
        h = hash_driver.create_content_hash(html_content)

        # first check if page is exact copy of already parsed documents
        if database_handler.find_page_duplicate(h):
            return True
        else:

            # create set of hash shingles
            # in order to prevent pages using lots of same tags to be treated as similar, remove html tags
            hash_set = hash_driver.text_to_shingle_set(self.remove_markups(html_content))

            # hash signature will be inserted to db later
            self.current_page["hash_signature"] = hash_set

            # calculate similarity between current document and already parsed documents using Jaccard similarity
            similarity = database_handler.calculate_biggest_similarity(hash_set)

            #print("SIMILARITY: ", similarity)

            return similarity > MAX_SIMILARITY

    """
       Remove markup tags from html content 
    """
    def remove_markups(self, html_content):
        return BeautifulSoup(html_content, "html.parser").text

    def add_page_to_frontier_array(self, page_url):
        page_domain = self.get_domain_url(page_url)

        if ALLOWED_DOMAIN in page_domain:
            # Only add pages in the allowed domain

            self.pages_to_add_to_frontier.append({
                "from": self.current_page["id"],
                "to": page_url
            })

    def quit(self):
        self.driver.quit()
