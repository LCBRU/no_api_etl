#!/usr/bin/env python3

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from api.environment import (
    EDGE_PASSWORD,
    EDGE_USERNAME,
    EDGE_BASE_URL,
)


def login(driver):
    driver.get(EDGE_BASE_URL)

    username = WebDriverWait(driver, 10).until(
        ec.presence_of_element_located((
            By.NAME, "fldUsername"))
    )

    password = driver.find_element_by_name('fldPassword')

    username.send_keys(EDGE_USERNAME)
    password.send_keys(EDGE_PASSWORD)

    password.send_keys(Keys.RETURN)

    WebDriverWait(driver, 10).until(
        ec.presence_of_element_located((By.ID, "headerOrganisationName"))
    )
