import schedule
import threading
import logging
import re
import time
import datetime
import traceback
import importlib
import pkgutil
import inspect
import os
from tempfile import mkstemp
from enum import Enum
from api.emailing import email_error
from api.selenium import SeleniumGrid
from .model import EtlTask, EtlTaskMessage
from .database import etl_central_session


class Schedule(Enum):
    @staticmethod
    def minutely(func):
        schedule.every().minute.do(func)

    @staticmethod
    def five_minutely(func):
        schedule.every(5).minutes.do(func)

    @staticmethod
    def hourly(func):
        schedule.every().hour.do(func)

    @staticmethod
    def daily(func):
        schedule.every().day.at("20:00").do(func)

    @staticmethod
    def daily_6pm(func):
        schedule.every().day.at("18:00").do(func)

    @staticmethod
    def daily_7pm(func):
        schedule.every().day.at("19:00").do(func)

    @staticmethod
    def daily_8pm(func):
        schedule.every().day.at("20:00").do(func)

    @staticmethod
    def daily_9pm(func):
        schedule.every().day.at("21:00").do(func)

    @staticmethod
    def daily_9_30pm(func):
        schedule.every().day.at("21:30").do(func)

    @staticmethod
    def daily_10pm(func):
        schedule.every().day.at("22:00").do(func)

    @staticmethod
    def daily_10_30pm(func):
        schedule.every().day.at("22:00").do(func)

    @staticmethod
    def daily_11pm(func):
        schedule.every().day.at("23:00").do(func)

    @staticmethod
    def on_saturday_mornings(func):
        schedule.every().saturday.at("08:00").do(func)

    @staticmethod
    def weekly(func):
        schedule.every().monday.at("08:00").do(func)

    @staticmethod
    def monthly(func):
        schedule.every(4).weeks.do(func)

    @staticmethod
    def daily_at_4am(func):
        schedule.every().day.at("04:00").do(func)

    @staticmethod
    def never(func):
        pass


class EtlStep:
    def __init__(self, name=None):
        # Unpick CamelCase
        self._name = name or type(self).__name__
        self._name = re.sub('([a-z])([A-Z])', r'\1 \2', self._name)

    def run(self):
        try:
            self.log_start()
            self.do_etl()
            self.log_end()

            logging.info("{} ran".format(self._name))

        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            self.log(
                message=e,
                attachment=traceback.format_exc(),
                log_level='ERROR',
            )
            logging.error(traceback.format_exc())
            email_error(self._name, traceback.format_exc())

    def log_start(self):
        with etl_central_session() as session:
            self._task = EtlTask(
                name=self._name,
                start_datetime=datetime.datetime.now()
            )
            session.add(self._task)
            session.commit()

    def log_end(self):
        end_datetime = datetime.datetime.now()
        with etl_central_session() as session:
            task = session.merge(self._task)
            task.end_datetime = end_datetime
            duration = (task.end_datetime - task.start_datetime).total_seconds()
            session.add(task)
            session.commit()


        if duration < 120:
            duration_message = '{:.1f} seconds'.format(duration)
        else:
            duration_message = '{:.0f} minutes'.format(duration // 60)

        self.log('Task {} ran for {}'.format(self._name, duration_message))

    def log(self, message, attachment=None, log_level='INFO'):
        with etl_central_session() as session:
            task = session.merge(self._task)
            task_message = EtlTaskMessage(
                etl_task_id=task.id,
                message_datetime=datetime.datetime.now(),
                message_type=log_level,
                message=message,
                attachment=attachment,
            )
            session.add(task_message)
            session.commit()
        
        level = getattr(logging, log_level.upper())
        logging.log(level, '{}: {}'.format(self._name, message))

    def do_etl(self):
        pass


class Etl(EtlStep):
    def __init__(self, name=None, schedule=None):
        super().__init__(name)

        self._schedule = schedule or Schedule.weekly

    def schedule(self):

        self._schedule(self.run)
        logging.info("{} scheduled".format(self._name))


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

        self.do_post_selenium_etl()

    def do_selenium_etl(self, driver):
        pass

    def do_post_selenium_etl(self):
        pass

def get_concrete_etls(cls=None):
    if (cls is None):
        cls = Etl

    logging.info(cls.__name__)

    result = [sub for sub in cls.__subclasses__()
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
        r().schedule()

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

        if e.__name__.lower() in exclude:
            continue

        if e.__name__[:len(etl_name)].lower() == etl_name.lower():
            try:
                e().run()
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
