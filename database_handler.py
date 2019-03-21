from multiprocessing import Lock
import psycopg2
from psycopg2 import pool
from config import config


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
                print('Connection to database created succesfully')

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    # TODO: insert all pages at once
    def insert_seed_page(self, seed_page):
        with self.lock:
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
                print(error)

    """
        This function uses the lock so that no two crawler processes have the same frontier url
    """

    def get_page_from_frontier(self):
        with self.lock:
            try:
                connection = self.connection_pool.getconn()

                # create a cursor
                cursor = connection.cursor()

                # execute a statement
                cursor.execute(
                    """
                        SELECT * FROM crawldb.page 
                        WHERE page_type_code='FRONTIER' AND active_in_frontier IS NULL
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
                        SET active_in_frontier=TRUE 
                        WHERE id=%s
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
                print(error)

    def add_pages_to_frontier(self, pages_to_add):
        print("Add pages", pages_to_add)

    def update_page(self, current_page):
        with self.lock:
            try:
                connection = self.connection_pool.getconn()

                cursor = connection.cursor()

                cursor.execute(
                    """
                        UPDATE crawldb.page 
                        SET site_id=%s, page_type_code=%s, html_content=%s, http_status_code=%s, 
                        accessed_time=%s, active_in_frontier=NULL 
                        WHERE id=%s
                    """,
                    (current_page["site_id"], current_page["page_type_code"], current_page["html_content"],
                     current_page["http_status_code"], current_page["accessed_time"], current_page["id"])
                )

                connection.commit()

                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

    def does_site_exist_in_db(self, url):
        connection = None

    def get_site(self, domain):
        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    SELECT * FROM crawldb.site WHERE domain=%s
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
                "robots_content": site[2]
            }
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def insert_site(self, site):
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

            cursor.close()

            return id
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def reset_frontier(self):
        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    UPDATE crawldb.page 
                    SET active_in_frontier=NULL 
                    WHERE page_type_code = 'FRONTIER'
                """
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def reset_database(self):
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

            cursor = connection.cursor()

            cursor.execute(
                "DELETE FROM crawldb.page"
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
                "DELETE FROM crawldb.site"
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
