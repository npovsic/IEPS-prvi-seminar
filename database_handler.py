from multiprocessing import Lock
import psycopg2
from psycopg2 import pool
from config import config
from datetime import datetime

# Maximum length of url (characters)
# https://stackoverflow.com/questions/417142/what-is-the-maximum-length-of-a-url-in-different-browsers
MAX_URL_LEN = 2000

class DatabaseHandler:
    def __init__(self, minimum_connections, max_connections):
        # Set a lock object, so that only one connection to the database is allowed
        self.lock = Lock()

        self.connection_pool = None

        try:
            # read connection parameters
            params = config()

            self.connection_pool = pool.ThreadedConnectionPool(
                minimum_connections,
                max_connections,
                user=params.get('user'),
                password=params.get('password'),
                host=params.get('host'),
                database=params.get('database')
            )

            if self.connection_pool:
                print('[DATABASE] Connection successfully established')

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE ESTABLISHING CONNECTION TO DATABASE]", error)

    """
        This function uses the lock so that no two crawler processes get the same frontier url
    """
    # TODO: Lock is obviously not working correctly
    def get_page_from_frontier(self):
        with self.lock:
            connection = None

            try:
                connection = self.connection_pool.getconn()

                # create a cursor
                cursor = connection.cursor()

                # execute a statement
                cursor.execute(
                    """
                        SELECT * FROM crawldb.page 
                        WHERE page_type_code='FRONTIER' AND active_in_crawler IS NULL
                        ORDER BY added_at_time
                    """
                )

                frontier = cursor.fetchone()

                if frontier is None:
                    return

                cursor.close()

                cursor = connection.cursor()

                cursor.execute(
                    """
                        UPDATE crawldb.page 
                        SET active_in_crawler=TRUE 
                        WHERE id=%s;
                    """,
                    (frontier[0],)
                )

                connection.commit()

                cursor.close()

                return {
                    'id': frontier[0],
                    'url': frontier[3]
                }
            except (Exception, psycopg2.DatabaseError) as error:
                print("[ERROR WHILE FETCHING PAGE FROM FRONTIER]", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    """
        Create a new entry in the links table
    """
    def link_pages(self, from_page, to_page):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.link("from_page", "to_page") 
                    VALUES(%s, %s);
                """,
                (from_page, to_page)
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE LINKING PAGES]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Add a single page to the frontier
    """
    def add_page_to_frontier(self, seed_page):
        connection = None

        if len(seed_page) <= MAX_URL_LEN:
            try:
                connection = self.connection_pool.getconn()

                cursor = connection.cursor()

                cursor.execute(
                    """
                        INSERT INTO crawldb.page("url", "page_type_code") 
                        VALUES(%s, %s);
                    """,
                    (seed_page, "FRONTIER")
                )

                connection.commit()

                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print("[ERROR WHILE ADDING PAGE TO FRONTIER]", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    """
        Add multiple pages to the frontier
    """
    def add_pages_to_frontier(self, pages_to_add):
        with self.lock:
            connection = None

            try:
                connection = self.connection_pool.getconn()

                for page in pages_to_add:

                    # avoid spider traps - if page's URL is longer than limit, do not add it to frontier
                    if len(page["to"]) <= MAX_URL_LEN:

                        try:
                            cursor = connection.cursor()

                            cursor.execute(
                                """
                                    INSERT INTO crawldb.page("url", "page_type_code", "added_at_time") 
                                    VALUES(%s, %s, %s)
                                    RETURNING id;
                                """,
                                (page["to"], "FRONTIER", datetime.now())
                            )

                            connection.commit()

                            to_page = cursor.fetchone()[0]

                            self.link_pages(page["from"], to_page)

                            cursor.close()

                        except psycopg2.IntegrityError:
                            # Do not print duplicate key errors (integrity error is thrown when inserting a duplicate url)

                            self.connection_pool.putconn(connection)

                            connection = self.connection_pool.getconn()
                        except (Exception, psycopg2.DatabaseError) as error:
                            print("[ERROR WHILE ADDING PAGES TO FRONTIER]", error)

                            self.connection_pool.putconn(connection)

                            connection = self.connection_pool.getconn()
            except (Exception, psycopg2.DatabaseError) as error:
                print("[ERROR WHILE ADDING PAGES TO FRONTIER]", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    """
        Remove the page from the frontier and populate all the necessary data
    """
    def remove_page_from_frontier(self, current_page):
        with self.lock:
            connection = None

            try:
                connection = self.connection_pool.getconn()

                cursor = connection.cursor()

                cursor.execute(
                    """
                        UPDATE crawldb.page 
                        SET site_id=%s, page_type_code=%s, html_content=%s, hash_content=%s, http_status_code=%s, 
                        accessed_time=%s, active_in_crawler=NULL 
                        WHERE id=%s;
                    """,
                    (current_page["site_id"], current_page["page_type_code"], current_page["html_content"], current_page["hash_content"],
                     current_page["http_status_code"], current_page["accessed_time"], current_page["id"])
                )

                connection.commit()

                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print("[ERROR WHILE REMOVING PAGE FROM FRONTIER]", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    """
        Find a page in the database by the hash_content and return it if it exists
    """

    def find_page_duplicate(self, hash_content):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT * FROM crawldb.page WHERE hash_content=%s;
                """,
                (hash_content,)
            )

            connection.commit()

            page = cursor.fetchone()

            return page is not None

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE CHECKING PAGE DUPLICATE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Find a site in the database by the domain name and return it if it exists
    """
    def get_site(self, domain):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT * FROM crawldb.site WHERE domain=%s;
                """,
                (domain,)
            )

            connection.commit()

            site = cursor.fetchone()

            if site is None:
                return

            cursor.close()

            return {
                "id": site[0],
                "domain": site[1],
                "robots_content": site[2]
            }
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE FETCHING SITE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Create a new site if it doesn't exist and return its id
    """
    def insert_site(self, site):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.site(domain, robots_content, sitemap_content)
	                VALUES (%s, %s, %s) RETURNING ID;
                """,
                (site["domain"], site["robots_content"], site["sitemap_content"])
            )

            connection.commit()

            id = cursor.fetchone()[0]

            if id is None:
                print("[ERROR WHILE INSERTING SITE] Inserted site was not found")

            cursor.close()

            return id
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE INSERTING SITE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Insert binary non-image page data
    """
    def insert_page_data(self, page_data):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.page_data(page_id, data_type_code, data)
                    VALUES (%s, %s, %s);
                """,
                (page_data["page_id"], page_data["data_type_code"], page_data["data"])
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE INSERTING PAGE DATA]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Insert the image that the crawler fetched
    """
    def insert_image_data(self, image_data):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.image(page_id, filename, content_type, data, accessed_time)
                    VALUES (%s, %s, %s, %s, %s);
                """,
                (image_data["page_id"], image_data["filename"], image_data["content_type"], image_data["data"],
                 image_data["accessed_time"])
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE INSERTING IMAGE DATA]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)


    """
        The crawler might have been shut down prematurely and some pages may have the active_in_crawler flag still set
        This function simply resets all active_in_crawler flags
    """
    def reset_frontier(self):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    UPDATE crawldb.page 
                    SET active_in_crawler=NULL 
                    WHERE page_type_code = 'FRONTIER';
                """
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE RESETTING FRONTIER]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Used for debugging only, as the name suggests it RESETS THE DATABASE TO ITS INITIAL STATE
    """
    def reset_database(self):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.image"
            )

            connection.commit()

            cursor.close()

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.link"
            )

            connection.commit()

            cursor.close()

            # create a cursor
            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.page_data"
            )

            connection.commit()

            cursor.close()

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.page"
            )

            connection.commit()

            cursor.close()

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.site"
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE RESETTING DATABASE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)
