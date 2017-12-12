"""
.. module: historical_reports.s3.config
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import os


class Config:
    """
    Class for maintaining the configuration for the entire runtime.
    By default, this will set most of the values from the environment variables -- but is also
    configurable via other means.

    Simply import this and use it:
    ```
    from s3.config import CONFIG
    ```
    """
    def __init__(self):
        self._s3_reports_version = 1    # Read-only -- this will update as the code changes.
        self._current_region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        self._exclude_fields = os.environ.get("EXCLUDE_FIELDS", "Name,_version").split(",")
        self._dump_to_buckets = os.environ.get("DUMP_TO_BUCKETS", "").split(",")
        self._dump_to_prefix = os.environ.get("DUMP_TO_PREFIX", "historical-s3-report.json")
        self._import_bucket = os.environ.get("IMPORT_BUCKET", None)
        self._import_prefix = os.environ.get("IMPORT_PREFIX", "historical-s3-report.json")
        self._export_if_missing = os.environ.get("EXPORT_IF_MISSING", False)

    @property
    def s3_reports_version(self):
        return self._s3_reports_version

    @property
    def current_region(self):
        return self._current_region

    @current_region.setter
    def current_region(self, region):
        self._current_region = region

    @property
    def exclude_fields(self):
        return self._exclude_fields

    @exclude_fields.setter
    def exclude_fields(self, fields):
        self._exclude_fields = fields

    @property
    def dump_to_buckets(self):
        return self._dump_to_buckets

    @dump_to_buckets.setter
    def dump_to_buckets(self, buckets):
        self._dump_to_buckets = buckets

    @property
    def dump_to_prefix(self):
        return self._dump_to_prefix

    @dump_to_prefix.setter
    def dump_to_prefix(self, prefix):
        self._dump_to_prefix = prefix

    @property
    def import_bucket(self):
        return self._import_bucket

    @import_bucket.setter
    def import_bucket(self, bucket):
        self._import_bucket = bucket

    @property
    def import_prefix(self):
        return self._import_prefix

    @import_prefix.setter
    def import_prefix(self, prefix):
        self._import_prefix = prefix

    @property
    def export_if_missing(self):
        return self._export_if_missing

    @export_if_missing.setter
    def export_if_missing(self, toggle):
        self._export_if_missing = toggle


CONFIG = Config()
