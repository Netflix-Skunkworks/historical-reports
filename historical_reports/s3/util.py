"""
.. module: historical_reports.s3.util
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import boto3
from botocore.exceptions import ClientError
from retrying import retry

from historical_reports.s3.config import CONFIG

import logging

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.INFO)


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def _upload_to_s3(file, client, bucket, prefix, content_type="application/json"):
    client.put_object(Bucket=bucket, Key=prefix, Body=file, ContentType=content_type)


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def _get_from_s3(client, bucket, prefix):
    try:
        return client.get_object(Bucket=bucket, Key=prefix)["Body"].read().decode()
    except ClientError as ce:
        if ce.response['Error']['Code'] == 'NoSuchKey':
            return None


def dump_to_s3(file):
    """
    This will dump the generated schema to S3.
    :param blob:
    :return:
    """
    client = boto3.client("s3")

    # Loop over each bucket and dump:
    for bucket in CONFIG.dump_to_buckets:
        log.debug("[ ] Dumping to {}/{}".format(bucket, CONFIG.dump_to_prefix))
        _upload_to_s3(file, client, bucket, CONFIG.dump_to_prefix)
        log.debug("[+] Complete")

    log.debug("[+] Completed dumping to all buckets.")


def fetch_from_s3():
    """
    This will fetch the report object from S3.
    :param bucket:
    :param prefix:
    :return:
    """
    client = boto3.client("s3")
    return _get_from_s3(client, CONFIG.import_bucket, CONFIG.import_prefix)


def set_config_from_input(lambda_input):
    """
    Sets the attributes on the configuration based on the input to the lambda function.

    These need to be set to the raw values that the configuration needs. For example, if the env var
    would take in a comma-separated-list, you would supply an actual list of the items, not the comma-separated-string.

    TODO: Make this a decorator.
    :param lambda_input:
    :return:
    """
    if lambda_input.get("config"):
        for attribute, value in lambda_input["config"].items():
            if hasattr(CONFIG, attribute):
                setattr(CONFIG, attribute, value)
            else:
                log.error("Config Attribute: {} is not valid.".format(attribute))
