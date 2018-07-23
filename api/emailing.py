#!/usr/bin/env python3

import smtplib
import os
from email.mime.text import MIMEText


EMAIL_FROM_ADDRESS = os.environ["EMAIL_FROM_ADDRESS"]
EMAIL_SMTP_SERVER = os.environ["EMAIL_SMTP_SERVER"]

DEFAULT_RECIPIENT = os.environ["DEFAULT_RECIPIENT"]


def email_error(report_name, error_text):
    msg = MIMEText(error_text)
    msg['Subject'] = 'Reporter: Error in ' + report_name
    msg['To'] = DEFAULT_RECIPIENT
    msg['From'] = EMAIL_FROM_ADDRESS

    s = smtplib.SMTP(EMAIL_SMTP_SERVER)
    s.send_message(msg)
    s.quit()


def get_recipients(recipients):
    result = set()

    list_of_recs = [os.getenv(r) for r in recipients]

    for lr in list_of_recs:
        if lr:
            result |= set(lr.split(','))

    if len(result) == 0:
        result = set([DEFAULT_RECIPIENT])

    return result
