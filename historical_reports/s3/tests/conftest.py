"""
.. module: historical_reports.s3.tests.conftest
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Mike Grima <mgrima@netflix.com>
"""
import json

import boto3
import pytest
from historical.s3.models import CurrentS3Model
from historical_reports.s3.models import S3ReportSchema
from moto import mock_dynamodb2, mock_s3
from historical_reports.s3.tests.factories import DynamoDBRecordFactory, DynamoDBDataFactory, \
    serialize, \
    UserIdentityFactory, RecordsFactory, SQSDataFactory, SnsDataFactory

S3_BUCKET = """{
    "arn": "arn:aws:s3:::testbucket{number}",
    "principalId": "joe@example.com",
    "userIdentity": {
        "sessionContext": {
            "userName": "TseXSKEYQrxm",
            "type": "Role",
            "arn": "arn:aws:iam::123456789012:role/historical_poller",
            "principalId": "AROAIKELBS2RNWG7KASDF",
            "accountId": "123456789012"
        },
        "principalId": "AROAIKELBS2RNWG7KASDF:joe@example.com",
        "type": "Service"
    },
    "eventSource": "aws.s3",
    "accountId": "123456789012",
    "eventTime": "2017-11-10T18:33:44Z",
    "eventSource": "aws.s3",
    "BucketName": "testbucket{number}",
    "Region": "us-east-1",
    "Tags": {
        "theBucketName": "testbucket{number}"
    },
    "configuration": {
        "Grants": {
            "75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a": [
                "FULL_CONTROL"
            ]
        },
        "LifecycleRules": [
            {
                "Expiration": {
                    "Date": "2015-01-01T00:00:00Z",
                    "Days": 123
                },
                "ID": "string",
                "Prefix": "string",
                "Status": "Enabled",
                "Transitions": [
                    {
                        "Date": "2015-01-01T00:00:00Z",
                        "Days": 123,
                        "StorageClass": "GLACIER"
                    }
                ]
            }
        ],
        "Logging": {},
        "Policy": null,
        "Tags": {
            "theBucketName": "testbucket{number}"
        },
        "Versioning": {},
        "Website": null,
        "Cors": [],
        "Notifications": {},
        "Acceleration": null,
        "Replication": {},
        "CreationDate": "2006-02-03T16:45:09Z",
        "AnalyticsConfigurations": [],
        "MetricsConfigurations": [],
        "InventoryConfigurations": [],
        "Name": "testbucket{number}",
        "_version": 8
    }
}"""


@pytest.fixture(scope='function')
def dynamodb():
    with mock_dynamodb2():
        yield boto3.client('dynamodb', region_name='us-east-1')


@pytest.fixture(scope='function')
def s3():
    with mock_s3():
        yield boto3.client('s3', region_name='us-east-1')


@pytest.fixture(scope='function')
def current_s3_table(dynamodb):
    yield CurrentS3Model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)


@pytest.fixture(scope="function")
def historical_table(current_s3_table):
    for x in range(0, 10):
        bucket = json.loads(S3_BUCKET.replace("{number}", "{}".format(x)))
        CurrentS3Model(**bucket).save()


@pytest.fixture(scope="function")
def dump_buckets(s3):
    for x in range(0, 10):
        s3.create_bucket(Bucket="dump{}".format(x))

    return s3


@pytest.fixture(scope="function")
def generated_file(historical_table):
    all_buckets = CurrentS3Model.scan()
    return S3ReportSchema(strict=True).dumps({"all_buckets": all_buckets}).data.encode("utf-8")


@pytest.fixture(scope="function")
def generated_report(generated_file):
    return json.loads(generated_file.decode("utf-8"))


@pytest.fixture(scope="function")
def bucket_event():
    new_bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            NewImage=new_bucket,
            Keys={
                'arn': new_bucket['arn']
            }
        ),
        eventName='INSERT'), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])
    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def delete_bucket_event():
    delete_bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))
    delete_bucket["configuration"] = {}

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            NewImage=delete_bucket,
            Keys={
                'arn': delete_bucket['arn']
            }
        ),
        eventName='MODIFY'), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])

    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def ttl_event():
    bucket = json.loads(S3_BUCKET.replace("{number}", "NEWBUCKET"))

    new_item = json.dumps(DynamoDBRecordFactory(
        dynamodb=DynamoDBDataFactory(
            OldImage=bucket,
            Keys={
                'arn': bucket['arn']
            }),
        eventName='REMOVE',
        userIdentity=UserIdentityFactory(
            type='Service',
            principalId='dynamodb.amazonaws.com'
        )), default=serialize)

    records = RecordsFactory(records=[SQSDataFactory(body=json.dumps(SnsDataFactory(Message=new_item),
                                                                     default=serialize))])

    return json.loads(json.dumps(records, default=serialize))


@pytest.fixture(scope="function")
def existing_s3_report(dump_buckets, generated_file):
    dump_buckets.put_object(Bucket="dump0", Key="historical-s3-report.json", Body=generated_file)

    return generated_file
