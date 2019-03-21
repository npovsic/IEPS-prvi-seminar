from multiprocessing import Lock
import psycopg2
from config import config


class DatabaseHandler:
    def __init__(self):
        # Set a lock object, so that only one connection to the database is allowed
        self.lock = Lock()

    # TODO: insert all pages at once
    def insert_seed_page(self, seed_page):
        with self.lock:
            connection = None

            try:
                # read connection parameters
                params = config()

                # connect to the PostgreSQL server
                print("Connecting to the PostgreSQL database...")
                connection = psycopg2.connect(**params)

                # create a cursor
                cursor = connection.cursor()

                # execute a statement
                cursor.execute("""INSERT INTO crawldb.page("url", "page_type_code")
                             VALUES('{}', '{}');""".format(seed_page.strip(), "FRONTIER")
                               )

                connection.commit()

                # close the communication with the PostgreSQL
                cursor.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if connection is not None:
                    connection.close()
                    print("Database connection closed.")

    """
        This function uses the lock so that no two crawler processes have the same frontier url
    """

    def get_frontier_page(self):
        with self.lock:
            connection = None

            try:
                # read connection parameters
                params = config()

                # connect to the PostgreSQL server
                print("Connecting to the PostgreSQL database...")
                connection = psycopg2.connect(**params)

                # create a cursor
                cursor = connection.cursor()

                # execute a statement
                cursor.execute(
                    "SELECT * FROM crawldb.page WHERE page_type_code = 'FRONTIER' AND active_in_frontier IS NULL")

                frontier = cursor.fetchone()

                if frontier is None:
                    return

                # close the communication with the PostgreSQL
                cursor.close()

                # create a cursor
                cursor = connection.cursor()

                # execute a statement
                cursor.execute(
                    "UPDATE crawldb.page SET active_in_frontier=TRUE WHERE id={}".format(frontier[0])
                )

                connection.commit()

                # close the communication with the PostgreSQL
                cursor.close()

                return {
                    'id': frontier[0],
                    'url': frontier[3]
                }
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if connection is not None:
                    connection.close()
                    print("Database connection closed.")

    def does_site_exist_in_db(self, url):
        connection = None

    def insert_site(self, query):
        connection = None

    def reset_frontier(self):
        connection = None

        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print("Connecting to the PostgreSQL database...")
            connection = psycopg2.connect(**params)

            # create a cursor
            cursor = connection.cursor()

            # execute a statement
            cursor.execute(
                "UPDATE crawldb.page SET active_in_frontier=NULL WHERE page_type_code = 'FRONTIER'"
            )

            connection.commit()

            # close the communication with the PostgreSQL
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                connection.close()
                print("Database connection closed.")