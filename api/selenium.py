"""Selenium grid connection manager
"""
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By


class SeleniumGrid():
    """Selenium grid connection manager
    """

    CHROME = DesiredCapabilities.CHROME
    FIREFOX = DesiredCapabilities.FIREFOX

    def __init__(self, browser):
        self.browser = browser

    def __enter__(self):
        self.driver = webdriver.Remote(
            command_executor='http://uhlbriccsapp02:4444/wd/hub',
            desired_capabilities=self.browser
        )
        self.driver.implicitly_wait(10)
        return self.driver

    def __exit__(self, *args):
        self.driver.quit()


def get_td_column_contents(tr, column):
    return tr.find_element_by_xpath(
        'td[position()={}]'.format(column)
        ).get_attribute('innerHTML')


def get_td_keyvalue_contents(table, key):
    row = table.find_elements(By.XPATH, 'tr[starts-with(td/text(), "{}") or starts-with(td/label/text(), "{}")]'.format(key, key))
    
    value = ''

    if len(row) > 0:
        value = row[0].find_element(By.XPATH, 'td[position()=2]').get_attribute('innerHTML')

    return value.strip()
