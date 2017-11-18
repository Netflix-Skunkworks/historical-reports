Historical Reports
=====================
[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)
[![Build Status](https://travis-ci.org/Netflix-Skunkworks/historical-reports.svg?branch=master)](https://travis-ci.org/Netflix-Skunkworks/historical-reports) 
[![Coverage Status](https://coveralls.io/repos/github/Netflix-Skunkworks/historical-reports/badge.svg)](https://coveralls.io/github/Netflix-Skunkworks/historical-reports)

## Historical-Reports is under heavy development and is not ready for production use.

This project contains Lambda functions that generate reports based on events produced by [Historical](https://github.com/Netflix-Skunkworks/historical).
These reports collate and transform data stored in the historical DynamoDB tables, and can publish events and notifications to other applications.

This project is organized into sub-directories with independent lambda functions. You may choose to deploy them independently
into your infrastructure.

The following reports are available:
1. S3
 

# Report Functions

## S3
[![PyPI version](https://badge.fury.io/py/historical-reports-s3.svg)](https://badge.fury.io/py/historical-reports-s3)
This is similar in nature to [SWAG](https://github.com/Netflix-Skunkworks/swag-client), this generates a global dictionary 
of all S3 buckets you have in your account, along with metadata about them. 
This report gets generated into JSON and stored into an S3 bucket(s) of your choosing. The primary
use of this is to know which buckets and which regions and accounts they reside in.

#### Permissions Required
The following IAM permissions are required for the S3 lambda function to execute:

    {
        "Statement": [
            {
                "Sid": "S3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject"
                ],
                "Resource": [
                    "arn:aws:s3:::<PREFIX-TO-HISTORICAL-DUMP-/LOCATIONS/HERE>"
                ]
            },
            {
                "Sid": "DynamoDB",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                "Resource": [
                    "arn:aws:dynamodb:<REGION>:<ACCOUNT-ID>:table/<HISTORICAL-S3-CURRENT-TABLE-HERE>"
                ]
            }
        ]
    }

# Deployment
Deployment can be achieved via the serverless tool. A sample serverless configuration is provided for each report type (under `serverless-examples/`).
Each report is designed to be an independent lambda function, which can be deployed to any number of AWS accounts and regions.

To begin deployment, you would need to first create a Python 3 virtual environment specific to the report function you want to deploy. Then,
grab AWS credentials to permit Serverless to:
1. Create/Modify/Delete CloudFormation templates
1. Create/Modify/Delete Lambda functions
1. Put and delete assets into/from a specific S3 bucket that contains the Lambda function
1. Create/Modify/Delete CloudWatch Logs and CloudWatch Event Rules
1. Create/Modify/Delete DynamoDB Streams

Once you have `serverless` installed, and are in an active Python 3 virtual environment, you can run `sls package -s STACK -r REGION`.
Once you verify that it has a sufficient configuration, you would then `sls deploy -s STAGE -r REGION` your deployment.
