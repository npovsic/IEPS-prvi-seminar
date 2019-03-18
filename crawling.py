from selenium import webdriver

from crawler import Crawler

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('headless')
driver = webdriver.Chrome(chrome_options = chrome_options)

driver.get('https://github.com/gingeleski')

print(driver.page_source)

driver.quit()