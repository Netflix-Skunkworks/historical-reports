"""
.. module: s3.generate
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

from raven_python_lambda import RavenLambdaWrapper
from historical.s3.models import CurrentS3Model

from s3.models import S3ReportSchema
from s3.util import dump_to_s3

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.WARNING)


def dump_report(commit=True):
    # Get all the data from DynamoDB:
    log.debug("Starting... Beginning scan.")
    all_buckets = CurrentS3Model.scan()

    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    # Dump to S3:
    if commit:
        log.debug("Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        dump_to_s3(json.dumps(generated_file, indent=4).replace("\"<empty>\"", "\"\"").encode("utf-8"))
    else:
        log.debug("Commit flag not set, not saving.")

    log.debug("Completed S3 report generation.")


@RavenLambdaWrapper()
def handler(event, context):
    """
    Historical S3 report generator. This will take a full dump of the Current S3 Historical table and generate
    a JSON with all of the data within it to the model.

    This generates the initial file. This need not be called often. Subsequent modifications to the generated JSON
    should occur via changes to the durable table (to respond to CRUDs).

    """
    dump_report()
