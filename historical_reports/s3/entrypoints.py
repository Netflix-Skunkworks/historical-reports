"""
.. module: historical_reports.s3.entrypoints
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging

from historical.constants import LOGGING_LEVEL
from historical.common.util import deserialize_records

from raven_python_lambda import RavenLambdaWrapper

from historical_reports.s3.generate import dump_report
from historical_reports.s3.update import update_records
from historical_reports.s3.util import set_config_from_input

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(LOGGING_LEVEL)


@RavenLambdaWrapper()
def handler(event, context):
    """
    Historical S3 report generator lambda handler. This will handle both scheduled events as well as dynamo stream
    events.
    """
    set_config_from_input(event)

    if event.get("Records"):
        log.debug('[@] Received update event with records.')

        # Deserialize the records:
        records = deserialize_records(event["Records"])
        log.debug('[ ] Received the (deserialized) records: {}'.format(records))

        # Update event:
        update_records(records)

    else:
        log.debug('[@] Received a scheduled event for a full report.')
        # Generate event:
        dump_report()
