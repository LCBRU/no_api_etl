"""Selenium grid connection manager
"""
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


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
