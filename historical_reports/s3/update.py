"""
.. module: historical_reports.s3.update
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json
import logging

from historical.constants import LOGGING_LEVEL, EVENT_TOO_BIG_FLAG
from historical.attributes import decimal_default
from historical.s3.models import CurrentS3Model

from historical_reports.s3.generate import dump_report
from historical_reports.s3.models import S3ReportSchema
from historical_reports.s3.util import fetch_from_s3, dump_to_s3
from historical_reports.s3.config import CONFIG

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(LOGGING_LEVEL)


def process_durable_event(record, s3_report):
    """Processes a group of Historical Durable Table events."""
    if record.get(EVENT_TOO_BIG_FLAG):
        result = list(CurrentS3Model.query(record['arn']))

        # Is the record too big and also not found in the Current Table? Then delete it:
        if not result:
            record['item'] = {'configuration': {}, 'BucketName': record['arn'].split('arn:aws:s3:::')[1]}

        else:
            record['item'] = dict(result[0])

    if not record['item']['configuration']:
        log.debug(f"[ ] Processing deletion for: {record['item']['BucketName']}")
        s3_report["buckets"].pop(record['item']['BucketName'], None)
    else:
        log.debug(f"[ ] Processing: {record['item']['BucketName']}")
        s3_report["all_buckets"].append(record['item'])


def update_records(records, commit=True):
    log.debug("[@] Starting Record Update.")

    # First, grab the existing JSON from S3:
    existing_json = fetch_from_s3()
    log.debug("[+] Grabbed all the existing data from S3.")

    # If the existing JSON is not present for some reason, then...
    if not existing_json:
        if commit and CONFIG.export_if_missing:
            CONFIG.dump_to_buckets = CONFIG.import_bucket.split(",")
            CONFIG.dump_to_prefix = CONFIG.import_prefix
            log.info("[!] The report does not exist. Dumping the full report to {}/{}".format(CONFIG.import_bucket,
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
        process_durable_event(record, report)

    # Serialize the data:
    generated_file = S3ReportSchema(strict=True).dump(report).data

    # Dump to S3:
    if commit:
        log.debug("[-->] Saving to S3.")

        # Replace <empty> with "" <-- Due to Pynamo/Dynamo issues...
        dump_to_s3(json.dumps(generated_file, indent=4, default=decimal_default).replace("\"<empty>\"", "\"\"").encode(
            "utf-8"))
    else:
        log.debug("[/] Commit flag not set, not saving.")

    log.debug("[@] Completed S3 report update.")
