"""
.. module: historical_reports.s3.tests.test_everything
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json

import pytest
from historical.common.util import deserialize_records

from historical_reports.s3.entrypoints import handler
from historical_reports.s3.config import CONFIG
from historical_reports.s3.generate import dump_report
from historical.s3.models import CurrentS3Model
from historical_reports.s3.models import S3ReportSchema
from historical_reports.s3.update import process_dynamodb_record, update_records
from historical_reports.s3.util import dump_to_s3, set_config_from_input


class MockContext:
    def get_remaining_time_in_millis(self):
        return 9000


def test_historical_table_fixture(historical_table):
    assert CurrentS3Model.count() == 10


def test_dump_buckets_fixture(dump_buckets):
    assert len(dump_buckets.list_buckets()["Buckets"]) == 10


def test_generated_file_fixture_is_json(generated_file):
    assert type(generated_file) is bytes
    assert len(json.loads(generated_file.decode("utf-8"))["buckets"]) == 10


def test_existing_s3_report_fixture(existing_s3_report, dump_buckets, generated_file):
    assert generated_file == dump_buckets.get_object(Bucket="dump0", Key="historical-s3-report.json")["Body"].read()


def test_bucket_schema(historical_table):
    all_buckets = CurrentS3Model.scan()
    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    assert generated_file["generated_date"]
    assert generated_file["s3_report_version"] == CONFIG.s3_reports_version
    assert not generated_file.get("all_buckets")

    for name, value in generated_file["buckets"].items():
        assert value["AccountId"] == "123456789012"
        assert value["Region"] == "us-east-1"
        assert value["Tags"]["theBucketName"] == name
        assert not value.get("_version")
        assert not value.get("Name")


def test_light_bucket_schema(historical_table):
    old_fields = CONFIG.exclude_fields
    CONFIG.exclude_fields = "Name,_version,Grants,LifecycleRules,Logging,Policy,Tags,Versioning,Website,Cors," \
                            "Notifications,Acceleration,Replication,CreationDate,AnalyticsConfigurations," \
                            "MetricsConfigurations,InventoryConfigurations".split(",")

    all_buckets = CurrentS3Model.scan()
    generated_file = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    assert generated_file["generated_date"]
    assert generated_file["s3_report_version"] == CONFIG.s3_reports_version
    assert len(generated_file["buckets"]) == 10
    assert not generated_file.get("all_buckets")

    for bucket in generated_file["buckets"].values():
        keys = bucket.keys()
        for excluded in CONFIG.exclude_fields:
            assert excluded not in keys

        assert bucket["AccountId"] == "123456789012"
        assert bucket["Region"] == "us-east-1"

    # Clean-up:
    CONFIG.exclude_fields = old_fields


def test_dump_to_s3(dump_buckets, generated_file):
    old_value = CONFIG.dump_to_buckets
    CONFIG.dump_to_buckets = ["dump{}".format(x) for x in range(0, 10)]

    dump_to_s3(generated_file)

    # Check that it's all good:
    for bucket in CONFIG.dump_to_buckets:
        assert dump_buckets.get_object(Bucket=bucket, Key=CONFIG.dump_to_prefix)["Body"].read() == generated_file

    # Clean-up:
    CONFIG.dump_to_buckets = old_value


@pytest.mark.parametrize("lambda_entry", [False, True])
def test_dump_report(dump_buckets, historical_table, lambda_entry):
    old_value = CONFIG.dump_to_buckets
    CONFIG.dump_to_buckets = ["dump{}".format(x) for x in range(0, 10)]

    if lambda_entry:
        handler({}, MockContext())
    else:
        dump_report()

    # Verify all the info:
    for bucket in CONFIG.dump_to_buckets:
        file = json.loads(dump_buckets.get_object(Bucket=bucket, Key=CONFIG.dump_to_prefix)["Body"].read().decode())

        assert file["generated_date"]
        assert file["s3_report_version"] == CONFIG.s3_reports_version
        assert not file.get("all_buckets")

        for name, value in file["buckets"].items():
            assert value["AccountId"] == "123456789012"
            assert value["Region"] == "us-east-1"
            assert value["Tags"]["theBucketName"] == name
            assert not value.get("_version")
            assert not value.get("Name")

    # Clean-up:
    CONFIG.dump_to_buckets = old_value


@pytest.mark.parametrize("change_type", ["INSERT", "MODIFY"])
def test_process_dynamodb_record(bucket_event, generated_report, change_type):
    bucket_event["Records"][0]["body"] = bucket_event["Records"][0]["body"].replace(
        '\"eventName\": \"INSERT\"', '\"eventName\": \"{}\"'.format(change_type))
    generated_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])

    process_dynamodb_record(records[0], generated_report)

    assert len(generated_report["all_buckets"]) == 1
    assert generated_report["all_buckets"][0].Region == "us-east-1"


def test_process_dynamodb_record_deletion(delete_bucket_event, generated_report):
    generated_report["all_buckets"] = []
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    # Should not do anything -- since not present in the list:
    assert not generated_report["all_buckets"]

    # Check if removal logic works:
    generated_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}

    # Standard "MODIFY" for deletion:
    delete_bucket_event["Records"][0]["eventName"] = "MODIFY"
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)
    assert not generated_report["buckets"].get("testbucketNEWBUCKET")


def test_process_dynamodb_deletion_event(delete_bucket_event, generated_report):
    generated_report["all_buckets"] = []
    generated_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}
    delete_bucket_event["Records"][0]["body"] = delete_bucket_event["Records"][0]["body"].replace(
        '\"eventName\": \"MODIFY\"', '\"eventName\": \"{}\"'.format("REMOVE"))
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    # Should not do anything -- since not present in the list:
    assert not generated_report["all_buckets"]

    # If we receive a removal event that is NOT from a TTL, that should remove the bucket.
    delete_bucket_event["Records"][0]["eventName"] = "REMOVE"
    records = deserialize_records(delete_bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)
    assert not generated_report["buckets"].get("testbucketNEWBUCKET")


def test_process_dynamodb_record_ttl(ttl_event, generated_report):
    generated_report["all_buckets"] = []
    records = deserialize_records(ttl_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    # Should not do anything -- since not present in the list:
    assert not generated_report["all_buckets"]

    generated_report["buckets"]["testbucketNEWBUCKET"] = {"some configuration": "this should be deleted"}
    process_dynamodb_record(records[0], generated_report)
    assert not generated_report["buckets"].get("testbucketNEWBUCKET")


def test_bucket_schema_for_events(historical_table, generated_report, bucket_event):
    generated_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    full_report = S3ReportSchema(strict=True).dump(generated_report).data

    assert full_report["generated_date"]
    assert full_report["s3_report_version"] == CONFIG.s3_reports_version
    assert not full_report.get("all_buckets")

    assert full_report["buckets"]["testbucketNEWBUCKET"]
    assert len(full_report["buckets"]) == 11

    for name, value in full_report["buckets"].items():
        assert value["AccountId"] == "123456789012"
        assert value["Region"] == "us-east-1"
        assert value["Tags"]["theBucketName"] == name
        assert not value.get("_version")
        assert not value.get("Name")


def test_lite_bucket_schema_for_events(historical_table, bucket_event):
    old_fields = CONFIG.exclude_fields
    CONFIG.exclude_fields = "Name,Owner,_version,Grants,LifecycleRules,Logging,Policy,Tags,Versioning,Website,Cors," \
                            "Notifications,Acceleration,Replication,CreationDate,AnalyticsConfigurations," \
                            "MetricsConfigurations,InventoryConfigurations".split(",")

    all_buckets = CurrentS3Model.scan()
    generated_report = S3ReportSchema(strict=True).dump({"all_buckets": all_buckets}).data

    generated_report["all_buckets"] = []
    records = deserialize_records(bucket_event["Records"])
    process_dynamodb_record(records[0], generated_report)

    lite_report = S3ReportSchema(strict=True).dump(generated_report).data

    assert lite_report["generated_date"]
    assert lite_report["s3_report_version"] == CONFIG.s3_reports_version
    assert not lite_report.get("all_buckets")

    assert lite_report["buckets"]["testbucketNEWBUCKET"]
    assert len(lite_report["buckets"]) == 11

    for bucket in lite_report["buckets"].values():
        keys = bucket.keys()
        for excluded in CONFIG.exclude_fields:
            assert excluded not in keys

        assert bucket["AccountId"] == "123456789012"
        assert bucket["Region"] == "us-east-1"

    # Clean-up:
    CONFIG.exclude_fields = old_fields


@pytest.mark.parametrize("lambda_entry", [False, True])
def test_update_records(existing_s3_report, historical_table, bucket_event, delete_bucket_event, dump_buckets,
                        lambda_entry):
    old_dump_to_buckets = CONFIG.dump_to_buckets
    old_import_bucket = CONFIG.import_bucket

    CONFIG.import_bucket = "dump0"
    CONFIG.dump_to_buckets = ["dump0"]

    # Add a bucket:
    if lambda_entry:
        handler(bucket_event, MockContext())
    else:
        records = deserialize_records(bucket_event["Records"])
        update_records(records)

    new_report = json.loads(
        dump_buckets.get_object(Bucket="dump0", Key="historical-s3-report.json")["Body"].read().decode("utf-8")
    )
    assert len(new_report["buckets"]) == 11

    existing_json = json.loads(existing_s3_report.decode("utf-8"))
    assert len(new_report["buckets"]) != len(existing_json["buckets"])
    assert new_report["buckets"]["testbucketNEWBUCKET"]

    # Delete a bucket:
    if lambda_entry:
        handler(delete_bucket_event, MockContext())
    else:
        records = deserialize_records(delete_bucket_event["Records"])
        update_records(records)

    delete_report = json.loads(
        dump_buckets.get_object(Bucket="dump0", Key="historical-s3-report.json")["Body"].read().decode("utf-8")
    )
    assert len(delete_report["buckets"]) == len(existing_json["buckets"]) == 10
    assert not delete_report["buckets"].get("testbucketNEWBUCKET")

    # Clean-up:
    CONFIG.dump_to_buckets = old_dump_to_buckets
    CONFIG.import_bucket = old_import_bucket


def test_update_records_sans_existing(historical_table, dump_buckets, bucket_event):
    old_dump_to_buckets = CONFIG.dump_to_buckets
    old_import_bucket = CONFIG.import_bucket
    old_export_if_missing = CONFIG.export_if_missing

    # First test that the object is missing, and we aren't going to perform a full report dump:
    CONFIG.import_bucket = "dump0"
    CONFIG.export_if_missing = False
    update_records(bucket_event["Records"])
    assert not dump_buckets.list_objects_v2(Bucket="dump0")["KeyCount"]

    # Now, with commit set to False:
    CONFIG.export_if_missing = True
    update_records(bucket_event["Records"], commit=False)
    assert not dump_buckets.list_objects_v2(Bucket="dump0")["KeyCount"]

    # Now with commit:
    update_records(bucket_event["Records"])
    assert len(dump_buckets.list_objects_v2(Bucket="dump0")["Contents"]) == 1

    # Clean-up:
    CONFIG.dump_to_buckets = old_dump_to_buckets
    CONFIG.import_bucket = old_import_bucket
    CONFIG.export_if_missing = old_export_if_missing


def test_configuration_update():
    # Just test that a handful are working:
    old_import_prefix = CONFIG.import_prefix
    old_current_region = CONFIG.current_region
    old_exclude_fields = CONFIG.exclude_fields

    lambda_config = {
        "config": {
            "import_prefix": "some-prefix",
            "exclude_fields": ["all", "of", "them"],
            "current_region": "us-west-2"
        }
    }

    set_config_from_input(lambda_config)

    assert CONFIG.import_prefix == "some-prefix"
    assert CONFIG.current_region == "us-west-2"
    assert CONFIG.exclude_fields == ["all", "of", "them"]

    # Clean-up:
    CONFIG.import_prefix = old_import_prefix
    CONFIG.current_region = old_current_region
    CONFIG.exclude_fields = old_exclude_fields
