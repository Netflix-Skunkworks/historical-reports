"""
.. module: historical_reports.s3.cli
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import logging

import click

from historical_reports.s3.config import CONFIG
from historical_reports.s3.generate import dump_report

logging.basicConfig()
log = logging.getLogger('historical-reports-s3')
log.setLevel(logging.DEBUG)


@click.group()
def cli():
    """Historical-Reports commandline for managing historical reports."""
    pass


def get_exclude_fields(ctx, param, fields):
    CONFIG.exclude_fields = fields.split(",")


def get_bucket(ctx, param, buckets):
    CONFIG.dump_to_buckets = buckets.split(",")


def get_dump_prefix(ctx, param, prefix):
    CONFIG.dump_to_prefix = prefix


@cli.command()
@click.option("--bucket", type=click.STRING, required=True, help="Comma separated list of S3 bucket to dump the "
                                                                 "report to.", callback=get_bucket)
@click.option("--exclude-fields", type=click.STRING, required=False, default="Name,_version",
              help="Comma separated top-level fields to not be included in the final report.",
              callback=get_exclude_fields)
@click.option("--dump-prefix", type=click.STRING, required=False, default="historical-s3-report.json",
              callback=get_dump_prefix)
@click.option("-c", "--commit", default=False, is_flag=True, help="Will only dump to S3 if commit flag is present")
def generate(bucket, exclude_fields, dump_prefix, commit):
    if not commit:
        log.warning("COMMIT FLAG NOT SET -- NOT SAVING ANYTHING TO S3!")
    dump_report(commit=commit)
