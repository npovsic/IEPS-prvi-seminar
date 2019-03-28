from database_handler import DatabaseHandler

"""
    Deletes all the data from the database
"""

database_handler = DatabaseHandler(1, 1)
database_handler.reset_database()
