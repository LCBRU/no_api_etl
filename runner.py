#!/usr/bin/env python3

import logging
import argparse
from api.core import run_all, schedule_etls, run_etls

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_parameters():
    parser = argparse.ArgumentParser(description='Run specific reports.')
    parser.add_argument(
        'report_names',
        metavar='report_names',
        nargs='*',
        help='Report names or start of the report name',
    )
    parser.add_argument(
        '-x',
        '--exclude',
        nargs='*',
        help='Reports names to exclude',
        default=[]
    )
    parser.add_argument(
        "-a",
        "--all",
        help="Run all reports",
        action="store_true",
    )

    args = parser.parse_args()

    return args

def run():
    logging.info("---- Starting ----")
    args = get_parameters()

    exclude = [x.lower() for x in args.exclude]

    if args.all:
        run_all(exclude)

        logging.info("---- All ETLs run ----")
    elif not args.report_names:
        schedule_etls()
    else:
        for report_name in args.report_names:
            run_etls(report_name, exclude)

        logging.info("---- All ETLs run ----")
