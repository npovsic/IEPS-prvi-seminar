###### PREREQUISITES:

Installing Selenium and Google Chrome Headless:

    - https://medium.com/war-code/getting-started-with-headless-chrome-on-windows-9e39c830dd78
    - https://medium.com/technowriter/install-selenium-on-mac-os-x-94c7a216aeb0

When starting the script, do not forget the use the Anaconda interpreter. (Jetbrains: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html)

Python multiprocessing:

    - https://docs.python.org/2/library/multiprocessing.html#multiprocessing-programming
    
    - https://sebastianraschka.com/Articles/2014_multiprocessing.html
    
PostgreSQL:

    - https://www.postgresql.org/download/
    
    - http://www.postgresqltutorial.com/postgresql-python/connect/
    
    - https://pynative.com/psycopg2-python-postgresql-connection-pooling/
    
    - http://initd.org/psycopg/docs/usage.html
    
Do not forget your password when installing postgreSQL, you need it to connect to the correct database

BeautifulSoup:
    
    - https://www.crummy.com/software/BeautifulSoup/bs4/doc/
    
Parsing domain names:

    - https://docs.python.org/3/library/urllib.parse.html
    
###### INSTRUCTIONS:
    
The main elements of this crawler are Selenium and Headless Chrome, which are used to render sites and 
then to scrape images and links. 

STEPS:

    1. Install all the required packages to your python interpreter
        - psycopg2
        - requests
        - selenium
        - bs4
        
    2. The chrome driver is already present in the directory, so you don't need to download it
    
    3. Start the crawler by running the start.py script
        - The script creates a Crawler class and runs it
        - The number of processes are set as an argument for the crawler
