from urllib.parse import unquote
import psycopg2
from psycopg2 import pool
from config import config
from datetime import datetime

"""
    Maximum length of url (characters)
    https://stackoverflow.com/questions/417142/what-is-the-maximum-length-of-a-url-in-different-browsers
"""
MAX_URL_LEN = 2000

MAX_BINARY_TABLE_SIZE = 1024 * 1024 * 1024  # 1GB

MAX_PAGES_TABLE_ROWS = 100000


class DatabaseHandler:
    def __init__(self, minimum_connections, max_connections):
        # Set a lock object, so that only one connection to the database is allowed

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

    def get_page_from_frontier(self, lock):
        lock.acquire()

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
                    LIMIT 1
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

            # Decode the url
            url = unquote(frontier[3])

            return {
                'id': frontier[0],
                'url': url,
                'html_content': None,
                'hash_content': None
            }
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE FETCHING PAGE FROM FRONTIER]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

            lock.release()

    """
        Return the page back to the frontier, used mainly for crawl delay purposes
    """

    def return_page_to_frontier(self, current_page):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    UPDATE crawldb.page 
                    SET active_in_crawler=NULL 
                    WHERE id=%s;
                """,
                (current_page["id"],)
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE RETURNING PAGE TO FRONTIER]", error)
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
                    SELECT * FROM crawldb.link
                    WHERE from_page=%s AND to_page=%s
                """,
                (from_page, to_page)
            )

            existing_link = cursor.fetchone()

            cursor.close()

            if existing_link is None:
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

    def add_seed_page_to_frontier(self, seed_page):
        connection = None

        if len(seed_page) <= MAX_URL_LEN:
            try:
                connection = self.connection_pool.getconn()

                cursor = connection.cursor()

                cursor.execute(
                    """
                        INSERT INTO crawldb.page("url", "page_type_code", "added_at_time") 
                        VALUES(%s, %s, %s);
                    """,
                    (seed_page, "FRONTIER", datetime.now())
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
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT COUNT(id) 
                    FROM crawldb.page 
                """
            )

            number_of_pages = cursor.fetchone()[0]

            cursor.close()

            if number_of_pages > MAX_PAGES_TABLE_ROWS:
                # The limit for the pages table has been reached

                return

            for page in pages_to_add:
                # avoid spider traps - if page's URL is longer than limit, do not add it to frontier
                if len(page["to"]) <= MAX_URL_LEN:
                    try:
                        cursor = connection.cursor()

                        cursor.execute(
                            """
                                SELECT * FROM crawldb.page 
                                WHERE url=%s
                            """,
                            (page["to"],)
                        )

                        # Check if the page already exists
                        to_page = cursor.fetchone()

                        cursor.close()

                        if to_page is None:
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

                            to_page = cursor.fetchone()

                            cursor.close()

                        self.link_pages(page["from"], to_page[0])
                    except psycopg2.IntegrityError:
                        print("[ERROR WHILE ADDING PAGES TO FRONTIER] Integrity error, adding a duplicate url")

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
                (current_page["site_id"], current_page["page_type_code"], current_page["html_content"],
                 current_page["hash_content"], current_page["http_status_code"], current_page["accessed_time"],
                 current_page["id"])
            )

            connection.commit()

            cursor.close()

            cursor = connection.cursor()

            cursor.execute(
                """
                    UPDATE crawldb.site 
                    SET last_crawled_at=%s
                    WHERE id=%s;
                """,
                (datetime.now(), current_page["site_id"])
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
        Insert hash signatures for current page
    """
    def insert_page_signatures(self, page_id, signatures):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.content_hash(page_id, hash, hash_length)
                    VALUES (%s, %s, %s);
                """,
                (page_id, str(signatures), len(signatures))
            )

            connection.commit()

            cursor.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE INSERTING HASH]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        Check out what is the percentage of similarity between current page containing set of hash signatures and 
        already crawled pages
    """
    def calculate_biggest_similarity(self, signatures):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                   SELECT * 
                   FROM   crawldb.content_hash i, LATERAL (
                        SELECT count(*) AS ct
                        FROM   unnest(i.hash) signature
                        WHERE  signature = ANY(%s::bigint[])
                    ) x, LATERAL (
                        SELECT (i.hash_length + %s) AS total_sum
                    ) y
                    ORDER  BY x.ct DESC, hash_length ASC; 
                """,
                (str(signatures), len(signatures))
            )

            connection.commit()

            first_matching = cursor.fetchone()

            if first_matching is None:
                return 0

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE FINDING SIMILAR PAGES]", error)
        finally:

            if connection:
                self.connection_pool.putconn(connection)

        if first_matching[4] > 0:

            # calculate jaccard similarity (intersection over union)
            return first_matching[4] / (first_matching[5] - first_matching[4])

        else:
            return 0

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
                "robots_content": site[2],
                "last_crawled_at": site[4]
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

            site_id = cursor.fetchone()[0]

            if site_id is None:
                print("[ERROR WHILE INSERTING SITE] Inserted site was not found")

            cursor.close()

            return site_id
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
                    SELECT SUM(data_size)
                    FROM crawldb.page_data
                """
            )

            size_of_page_data_table = cursor.fetchone()[0]

            if size_of_page_data_table is None:
                size_of_page_data_table = 0

            cursor.close()

            if (size_of_page_data_table + page_data["data_size"]) >= MAX_BINARY_TABLE_SIZE:
                # The size limit set for the table has been reached

                return

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.page_data(page_id, data_type_code, data, data_size)
                    VALUES (%s, %s, %s, %s);
                """,
                (page_data["page_id"], page_data["data_type_code"], page_data["data"], page_data["data_size"])
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
                    SELECT SUM(data_size)
                    FROM crawldb.image
                """
            )

            size_of_images_table = cursor.fetchone()[0]

            if size_of_images_table is None:
                size_of_images_table = 0

            cursor.close()

            if (size_of_images_table + image_data["data_size"]) >= MAX_BINARY_TABLE_SIZE:
                # The size limit set for the table has been reached

                return

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.image(page_id, filename, content_type, data, data_size, accessed_time)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """,
                (image_data["page_id"], image_data["filename"], image_data["content_type"], image_data["data"],
                 image_data["data_size"], image_data["accessed_time"])
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

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.content_hash"
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE RESETTING DATABASE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def fetch_all_sites(self):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT * FROM crawldb.site
                """
            )

            connection.commit()

            return cursor.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE FETCHING SITE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def fetch_pages_by_site(self, site_id):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT * FROM crawldb.page WHERE site_id=%s
                """,
                (site_id,)
            )

            connection.commit()

            return cursor.fetchall()

        except (Exception, psycopg2.DatabaseError) as error:
            print("[ERROR WHILE FETCHING SITE]", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)