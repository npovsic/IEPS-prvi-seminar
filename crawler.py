from multiprocessing import Process
import requests
from selenium import webdriver

class Crawler:
    def __init__(self, number_of_threads):
        print('created crawler')

        self.number_of_threads = number_of_threads

    def run(self):
        for i in range(self.number_of_threads):
            p = Process(target=self.create_process)
            p.start()

    def create_process(self):
        crawler_process = CrawlerProcess()

        crawler_process.render_in_headless('https://www.google.com')


class CrawlerProcess:
    def __init__(self):
        print('created process')

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('headless')
        self.driver = webdriver.Chrome(chrome_options=chrome_options)

    def fetch_url(self, url):
        response = requests.get(url)

        print(response)

    def render_in_headless(self, url):
        self.driver.get(url)

        print(self.driver.page_source)

    def quit(self):
        self.driver.quit()

