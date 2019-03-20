from multiprocessing import Process
import requests
from selenium import webdriver
import psycopg2
from psycopg2 import pool
from config import config


def connect_to_db(minimum_connections, max_connections):
    # we will create a new threaded connection pool, which will be used by multiple processes
    connection_pool = None

    try:
        # read connection parameters
        params = config()

        connection_pool = pool.ThreadedConnectionPool(
            minimum_connections,
            max_connections,
            user=params.get('user'),
            password=params.get('password'),
            host=params.get('host'),
            database=params.get('database')
        )

        if connection_pool:
            print('Connection to database created succesfully')

        conn = connection_pool.getconn()

        ps_cursor = conn.cursor()
        ps_cursor.execute("select * from crawldb.data_type")
        mobile_records = ps_cursor.fetchmany(2)
        print ("Displaying rows from mobile table")
        for row in mobile_records:
            print (row)
        ps_cursor.close()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection_pool:
            connection_pool.closeall
        print("Connection to database is now closed")

class Crawler:
    def __init__(self, number_of_processes):
        print('created crawler')

        self.number_of_processes = number_of_processes

        connect_to_db(1, 2)

    def run(self):
        for i in range(self.number_of_processes):
            p = Process(target=self.create_process)
            p.start()

    def create_process(self):
        crawler_process = CrawlerProcess()

        # crawler_process.render_in_headless('https://www.google.com')


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

