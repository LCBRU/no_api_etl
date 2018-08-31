#!/usr/bin/env python3

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.encoders import encode_base64
from email.mime.text import MIMEText
from api.environment import (
    EMAIL_FROM_ADDRESS,
    EMAIL_SMTP_SERVER,
    DEFAULT_RECIPIENT,
)


def email_error(report_name, error_text, screenshot=None):
    msg = MIMEMultipart()
    msg['Subject'] = 'Reporter: Error in ' + report_name
    msg['To'] = DEFAULT_RECIPIENT
    msg['From'] = EMAIL_FROM_ADDRESS

    msg.attach(MIMEText(error_text))

    if screenshot:
        part = MIMEBase('image', 'png')
        part.set_payload(screenshot)
        encode_base64(part)

        part.add_header('Content-Disposition',
                        'attachment; filename="screenshot.png"')

        msg.attach(part)

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
