"""
.. module: historical_reports.s3.update
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

from historical.attributes import decimal_default
from historical.common.dynamodb import deserialize_current_dynamo_to_pynamo
from historical.s3.models import CurrentS3Model

from historical_reports.s3.generate import dump_report
from historical_reports.s3.models import S3ReportSchema
from historical_reports.s3.util import fetch_from_s3, dump_to_s3
from historical_reports.s3.config import CONFIG

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.INFO)


def process_dynamodb_record(record, s3_report):
    """Processes a group of DynamoDB NewImage records."""
    if record['eventName'] in ['INSERT', 'MODIFY']:
        # Serialize that specific report:
        modifed_bucket = deserialize_current_dynamo_to_pynamo(record, CurrentS3Model)

        # Is this a soft-deletion? (Config set to {})?
        if not modifed_bucket.configuration.attribute_values:
            s3_report["buckets"].pop(modifed_bucket.BucketName, None)
        else:
            s3_report["all_buckets"].append(modifed_bucket)

    if record['eventName'] == 'REMOVE':
        # We are *ONLY* tracking the deletions from the DynamoDB TTL service.
        # Why? Because when we process deletion records, we are first saving a new "empty" revision to the "Current"
        # table. The "empty" revision will then trigger this Lambda as a "MODIFY" event. Then, right after it saves
        # the "empty" revision, it will then delete the item from the "Current" table. At that point,
        # we have already saved the "deletion revision" to the "Historical" table. Thus, no need to process
        # the deletion events -- except for TTL expirations (which should never happen -- but if they do, you need
        # to investigate why...)
        if record.get('userIdentity'):
            if record['userIdentity']['type'] == 'Service':
                if record['userIdentity']['principalId'] == 'dynamodb.amazonaws.com':
                    log.debug("Processing TTL deletion.")
                    s3_report["buckets"].pop(record['dynamodb']['OldImage']["BucketName"]["S"], None)


def update_records(records, commit=True):
    log.debug("Starting Record Update.")

    # First, grab the existing JSON from S3:
    existing_json = fetch_from_s3()
    log.debug("Grabbed all the existing data from S3.")

    # If the existing JSON is not present for some reason, then...
    if not existing_json:
        if commit and CONFIG.export_if_missing:
            CONFIG.dump_to_buckets = CONFIG.import_bucket.split(",")
            CONFIG.dump_to_prefix = CONFIG.import_prefix
            log.info("The report does not exist. Dumping the full report to {}/{}".format(CONFIG.import_bucket,
                                                                                          CONFIG.import_prefix))
            dump_report()

        else:
            log.error("[X] The existing log was not present and the `EXPORT_IF_MISSING` env var was "
                      "not set so exiting.")

        return

    # Deserialize the report:
    report = S3ReportSchema().loads(existing_json).data
    report["all_buckets"] = []

    for record in records:
        process_dynamodb_record(record, report)

    # Serialize the data:
    generated_file = S3ReportSchema(strict=True).dump(report).data

    # Dump to S3:
    if commit:
        log.debug("Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        dump_to_s3(json.dumps(generated_file, indent=4, default=decimal_default).replace("\"<empty>\"", "\"\"").encode(
            "utf-8"))
    else:
        log.debug("Commit flag not set, not saving.")

    log.debug("Completed S3 report update.")
