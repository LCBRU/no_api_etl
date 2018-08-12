#!/usr/bin/env python3

from urllib import parse
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.environment import (
    CRFM_PASSWORD,
    CRFM_USERNAME,
    CRFM_BASE_URL,
)

def login(driver):
    driver.get(parse.urljoin(
            CRFM_BASE_URL,
            'Login.aspx',
        ))

    username = WebDriverWait(driver, 10).until(
        ec.presence_of_element_located((
            By.ID, "tbLogin"))
    )

    password = driver.find_element(By.ID, 'tbPassword')

    username.send_keys(CRFM_USERNAME)
    password.send_keys(CRFM_PASSWORD)

    password.send_keys(Keys.RETURN)

    WebDriverWait(driver, 10).until(
        ec.presence_of_element_located((By.CSS_SELECTOR, "div.pnl_primary_links"))
    )
