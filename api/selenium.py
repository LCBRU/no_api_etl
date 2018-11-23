"""Selenium grid connection manager
"""
import os
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By

SELENIUM_HOST = os.environ.get("SELENIUM_HOST", '')
SELENIUM_PORT = os.environ.get("SELENIUM_PORT", '4444')


class SeleniumGrid():
    """Selenium grid connection manager
    """

    CHROME = DesiredCapabilities.CHROME
    FIREFOX = DesiredCapabilities.FIREFOX

    def __init__(self, browser):
        self.browser = browser

    def __enter__(self):
        self.driver = webdriver.Remote(
            command_executor='http://{}:{}/wd/hub'.format(SELENIUM_HOST, SELENIUM_PORT),
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


def get_td_keyvalue_pairs(tbody, key_is_label=False):
    result = {}

    for tr in tbody.find_elements_by_tag_name('tr'):
        td = tr.find_elements_by_tag_name('td')

        if key_is_label:
            key_container = td[0].find('label')
        else:
            key_container = td[0]

        key = key_container.get_attribute('innerHTML').strip()
        value = td[1]

        result[key] = value

    return result


def get_td_keyvalue_contents(table, key):
    row = table.find_elements(By.XPATH, 'tr[starts-with(td/text(), "{}") or starts-with(td/label/text(), "{}")]'.format(key, key))
    
    value = ''

    if len(row) > 0:
        value = row[0].find_element(By.XPATH, 'td[position()=2]').get_attribute('innerHTML')

    return value.strip()
