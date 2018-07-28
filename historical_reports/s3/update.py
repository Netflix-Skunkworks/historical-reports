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
from historical.s3.models import CurrentS3Model
from historical.common.dynamodb import deserialize_durable_record_to_current_model

from historical_reports.s3.generate import dump_report
from historical_reports.s3.models import S3ReportSchema
from historical_reports.s3.util import fetch_from_s3, dump_to_s3
from historical_reports.s3.config import CONFIG

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.INFO)


def process_dynamodb_record(record, s3_report):
    """
    Processes a group of DynamoDB NewImage records.

    This logic is largely copy and pasted from Historical with few modifications.
    """
    if record['eventName'] == 'REMOVE':
        # This logic is copied and pasted from Historical. The Durable table does not (yet? maybe? never?) have
        # TTLs. As such, this should never happen here:
        # if record.get('userIdentity'):
        #     if record['userIdentity']['type'] == 'Service':
        #         if record['userIdentity']['principalId'] == 'dynamodb.amazonaws.com':
        #             s3_report["buckets"].pop(record['dynamodb']['OldImage']["BucketName"]["S"], None)
        #             log.error("[?] Processing TTL deletion for ARN/Event Time: {}/{} in the "
        #                       "Durable table. This is odd...".format(
        #                         record['dynamodb']['Keys']['arn']['S'],
        #                         record['dynamodb']['OldImage']['eventTime']['S']))

        # This should **NOT** be happening in the Durable table... If it does, we need to raise an exception:
        # else:
        s3_report["buckets"].pop(record['dynamodb']['OldImage']["BucketName"]["S"], None)
        log.error('[?] Item with ARN/Event Time: {}/{} was deleted from the Durable table.'
                  ' This is odd...'.format(record['dynamodb']['Keys']['arn']['S'],
                                           record['dynamodb']['OldImage']['eventTime']['S']))

    if record['eventName'] in ['INSERT', 'MODIFY']:
        # Serialize that specific report:
        modified_bucket = deserialize_durable_record_to_current_model(record, CurrentS3Model)

        # If the current object is too big for SNS, and it's not in the current table, then delete it.
        # -- OR -- if this a soft-deletion? (Config set to {})
        if not modified_bucket or not modified_bucket.configuration.attribute_values:
            s3_report["buckets"].pop(record['dynamodb']['NewImage']["BucketName"]["S"], None)
        else:
            s3_report["all_buckets"].append(modified_bucket)


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
