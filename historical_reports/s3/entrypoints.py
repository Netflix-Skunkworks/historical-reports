"""
.. module: historical_reports.s3.entrypoints
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
from historical.common.util import deserialize_records
from raven_python_lambda import RavenLambdaWrapper
from historical_reports.s3.generate import dump_report
from historical_reports.s3.update import update_records
from historical_reports.s3.util import set_config_from_input


@RavenLambdaWrapper()
def handler(event, context):
    """
    Historical S3 report generator lambda handler. This will handle both scheduled events as well as dynamo stream
    events.
    """
    set_config_from_input(event)

    if event.get("Records"):
        # Deserialize the records:
        records = deserialize_records(event["Records"])

        # Update event:
        update_records(records)

    else:
        # Generate event:
        dump_report()
