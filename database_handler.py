from multiprocessing import Lock
import psycopg2
from psycopg2 import pool
from config import config
from datetime import datetime

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
            print("ERROR IN DATABASE", error)

    def insert_seed_page(self, seed_page):
        connection = None

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    """
        This function uses the lock so that no two crawler processes get the same frontier url
    """
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
                        WHERE page_type_code='FRONTIER' AND active_in_frontier IS NULL
                        ORDER BY added_to_frontier
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
                    'url': frontier[3],
                    'added': frontier[7],
                }
            except (Exception, psycopg2.DatabaseError) as error:
                print("ERROR IN DATABASE", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    def link_pages(self, from_page, to_page):
        connection = None

        try:
            connection = self.connection_pool.getconn()

            cursor = connection.cursor()

            cursor.execute(
                """
                    INSERT INTO crawldb.link("from_page", "to_page") 
                    VALUES(%s, %s)
                """,
                (from_page, to_page)
            )

            connection.commit()

            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def add_pages_to_frontier(self, pages_to_add):
        with self.lock:
            connection = None

            try:
                connection = self.connection_pool.getconn()

                for page in pages_to_add:
                    try:
                        cursor = connection.cursor()

                        cursor.execute(
                            """
                                INSERT INTO crawldb.page("url", "page_type_code", "added_to_frontier") 
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
                        # Do not print duplicate key errors

                        self.connection_pool.putconn(connection)

                        connection = self.connection_pool.getconn()
                    except (Exception, psycopg2.DatabaseError) as error:
                        print("ERROR IN DATABASE", error)

                        self.connection_pool.putconn(connection)

                        connection = self.connection_pool.getconn()
            except (Exception, psycopg2.DatabaseError) as error:
                print("ERROR IN DATABASE", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    def update_page(self, current_page):
        with self.lock:
            connection = None

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

                print("[TRANSFERRED PAGE FROM FRONTIER]", current_page["url"])

                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print("ERROR IN DATABASE", error)
            finally:
                if connection:
                    self.connection_pool.putconn(connection)

    def get_site(self, domain):
        connection = None

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

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

            cursor.close()

            return id
        except (Exception, psycopg2.DatabaseError) as error:
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

    def reset_frontier(self):
        connection = None

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)

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
            print("ERROR IN DATABASE", error)
        finally:
            if connection:
                self.connection_pool.putconn(connection)
