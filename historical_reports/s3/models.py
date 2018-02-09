"""
.. module: historical_reports.s3.models
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
from marshmallow import Schema, fields
from marshmallow.fields import Field
from datetime import datetime

from historical_reports.s3.config import CONFIG

import logging

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.INFO)


def get_generated_time(*args):
    return datetime.utcnow().replace(tzinfo=None, microsecond=0).isoformat() + "Z"


def _serialize_bucket(bucket, account_id, region):
    bucket["AccountId"] = account_id
    bucket["Region"] = region

    # Remove fields in the exclusion list:
    for e in CONFIG.exclude_fields:
        bucket.pop(e, None)

    return bucket


class BucketField(Field):
    def _serialize(self, value, attr=None, data=None):
        buckets = data.get("buckets", {})
        for b in data["all_buckets"]:
            log.debug("Fetched details for bucket: {}".format(b.arn))
            name = b.configuration.attribute_values["Name"]  # Store because removed by default.

            # Add the bucket:
            buckets[name] = _serialize_bucket(b.configuration.attribute_values, b.accountId, b.Region)

        return buckets

    def _deserialize(self, value, attr, data):
        return {name: details for name, details in data["buckets"].items()}


class S3ReportSchema(Schema):
    s3_report_version = fields.Int(dump_only=True, required=True, default=CONFIG.s3_reports_version)
    generated_date = fields.Function(get_generated_time, required=True)
    all_buckets = BucketField(required=True, dump_to="buckets")
    buckets = BucketField(required=True, load_from="buckets", load_only=True)
