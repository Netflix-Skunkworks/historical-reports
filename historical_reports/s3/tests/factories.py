"""
.. module: s3-report.tests.factories
    :platform: Unix
    :copyright: (c) 2017 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. author:: Kevin Glisson <kglisson@netflix.com>
"""
# Copy and pasted the factories from Historical.
import datetime

from boto3.dynamodb.types import TypeSerializer
from factory import Factory, SubFactory, post_generation
from factory.fuzzy import FuzzyText

seria = TypeSerializer()


def serialize(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        serial = obj.replace(microsecond=0).replace(tzinfo=None).isoformat() + "Z"
        return serial

    if isinstance(obj, bytes):
        return obj.decode('utf-8')

    return obj.__dict__


class Records(object):
    def __init__(self, records):
        self.Records = records


class SessionIssuer(object):
    def __init__(self, userName, type, arn, principalId, accountId):
        self.userName = userName
        self.type = type
        self.arn = arn
        self.principalId = principalId
        self.accountId = accountId


class SessionIssuerFactory(Factory):
    class Meta:
        model = SessionIssuer

    userName = FuzzyText()
    type = 'Role'
    arn = 'arn:aws:iam::123456789012:role/historical_poller'
    principalId = 'AROAIKELBS2RNWG7KASDF'
    accountId = '123456789012'


class UserIdentity(object):
    def __init__(self, sessionContext, principalId, type):
        self.sessionContext = sessionContext
        self.principalId = principalId
        self.type = type


class UserIdentityFactory(Factory):
    class Meta:
        model = UserIdentity

    sessionContext = SubFactory(SessionIssuerFactory)
    principalId = 'AROAIKELBS2RNWG7KASDF:joe@example.com'
    type = 'Service'


class DynamoDBData(object):
    def __init__(self, NewImage, OldImage, Keys):
        self.OldImage = {k: seria.serialize(v) for k, v in OldImage.items()}
        self.NewImage = {k: seria.serialize(v) for k, v in NewImage.items()}
        self.Keys = {k: seria.serialize(v) for k, v in Keys.items()}


class DynamoDBDataFactory(Factory):
    class Meta:
        model = DynamoDBData

    NewImage = {}
    Keys = {}
    OldImage = {}


class DynamoDBRecord(object):
    def __init__(self, dynamodb, eventName, userIdentity):
        self.dynamodb = dynamodb
        self.eventName = eventName
        self.userIdentity = userIdentity


class DynamoDBRecordFactory(Factory):
    """Factory generating a DynamoDBRecord"""
    class Meta:
        model = DynamoDBRecord

    dynamodb = SubFactory(DynamoDBDataFactory)
    eventName = 'INSERT'
    userIdentity = SubFactory(UserIdentityFactory)


class DynamoDBRecordsFactory(Factory):
    class Meta:
        model = Records

    @post_generation
    def Records(self, create, extracted, **kwargs):
        if not create:
            # Simple build, do nothing.
            return

        if extracted:
            # A list of groups were passed in, use them
            for record in extracted:
                self.Records.append(record)
