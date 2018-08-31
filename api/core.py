import schedule
import logging
import re
import time
import traceback
import importlib
import pkgutil
import inspect
import os
from tempfile import mkstemp
from enum import Enum
from api.emailing import email_error
from api.selenium import SeleniumGrid


class Schedule(Enum):
    @staticmethod
    def daily(func):
        schedule.every().day.at("08:00").do(func)

    @staticmethod
    def weekly(func):
        schedule.every().monday.at("08:00").do(func)

    @staticmethod
    def monthly(func):
        schedule.every(4).weeks.do(func)

    @staticmethod
    def never(func):
        pass


class Etl:
    def __init__(self, name=None, schedule=None):

        # Unpick CamelCase
        self._name = name or type(self).__name__
        self._name = re.sub('([a-z])([A-Z])', r'\1 \2', self._name)
        self._schedule = schedule or Schedule.weekly

    def schedule(self):

        self._schedule(self.run)
        logging.info("{} scheduled".format(self._name))

    def run(self):
        try:
            self.do_etl()

            logging.info("{} ran".format(self._name))

        except KeyboardInterrupt as e:
            raise e
        except Exception:
            logging.error(traceback.format_exc())
            email_error(self._name, traceback.format_exc())

    def do_etl(self):
        pass


class SeleniumEtl(Etl):

    def __init__(self, name=None, schedule=None, browser=SeleniumGrid.CHROME):
        super().__init__(name, schedule)
        self._browser = browser

    def do_etl(self):
        with SeleniumGrid(self._browser) as driver:
            try:
                self.do_selenium_etl(driver)
            except KeyboardInterrupt as e:
                raise e
            except Exception:
                logging.error(traceback.format_exc())

                email_error(
                    report_name=self._name,
                    error_text=traceback.format_exc(),
                    screenshot=driver.get_screenshot_as_png(),
                )

    def do_selenium_etl(self, driver):
        pass


def get_concrete_etls(cls=None):

    if (cls is None):
        cls = Etl

    result = [sub() for sub in cls.__subclasses__()
              if len(sub.__subclasses__()) == 0 and
              # If the constructor requires parameters
              # other than self (i.e., it has more than 1
              # argument), it's an abstract class
              len(inspect.getfullargspec(sub.__init__)[0]) == 1]

    for sub in [sub for sub in cls.__subclasses__()
                if len(sub.__subclasses__()) != 0]:
        result += get_concrete_etls(sub)

    return result


def schedule_etls():
    reports = get_concrete_etls()

    for r in reports:
        r.schedule()

    logging.info("---- {} reports scheduled ----".format(len(reports)))

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info('Schedule stopped')
            return


def run_etls(etl_name, exclude):
    etls = get_concrete_etls()

    for e in etls:

        if type(e).__name__.lower() in exclude:
            continue

        if type(e).__name__[:len(etl_name)].lower() == etl_name.lower():
            try:
                e.run()
            except KeyboardInterrupt:
                logging.info('Schedule stopped')
                return


def run_all(exclude):
    etls = get_concrete_etls()

    for e in etls:

        if type(e).__name__.lower() in exclude:
            continue

        e.run()


def get_sub_modules(path, prefix):
    result = []

    for m in pkgutil.iter_modules(path):
        new_module_name = prefix + m[1]
        result.append(new_module_name)
        result.extend(get_sub_modules(
            [path[0] + '/' + m[1]],
            new_module_name + '.'
        ))

    return result


def import_sub_etls(path, name):
    for m in get_sub_modules(path, name + '.'):
        importlib.import_module(m)
