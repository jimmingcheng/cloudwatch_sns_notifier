import boto3
import json
import logging
import os
from base64 import b64decode
from datetime import datetime
from gzip import GzipFile
from io import BytesIO


SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

EMAIL_BODY = """
{log_events}

Raw logs:
--------------------------------
{raw_logs}
"""


LOG_EVENT_FOR_EMAIL_BODY = """
At {timestamp}
--------------------------------
{message}
"""


def lambda_handler(event, context):
    log_data = get_log_data(event)

    logging.error(log_data)

    boto3.client('sns').publish(
        TargetArn=SNS_TOPIC_ARN,
        Subject=get_subject(log_data),
        Message=get_message(log_data),
    )


def get_log_data(event):
    enc_log_data = event['awslogs']['data']
    log_data_str = GzipFile(fileobj=BytesIO(b64decode(enc_log_data))).read()
    log_data = json.loads(log_data_str)
    return log_data


def get_subject(log_data):
    return 'Error from ' + log_data['logGroup']


def get_message(log_data):
    log_events = ''.join(
        LOG_EVENT_FOR_EMAIL_BODY.format(
            timestamp=_format_timestamp(event['timestamp']),
            message=event['message'].replace('\\n', '\n'),
        )
        for event in log_data['logEvents']
    )

    return EMAIL_BODY.format(
        log_events=log_events,
        raw_logs=json.dumps(log_data),
    )


def _format_timestamp(timestamp_ms):
    return datetime.fromtimestamp(
        timestamp_ms / 1000
    ).strftime(
        '%Y-%m-%d %H:%M:%S'
    )
