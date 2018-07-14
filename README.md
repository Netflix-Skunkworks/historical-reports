Historical Reports
=====================
[![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)
[![Build Status](https://travis-ci.org/Netflix-Skunkworks/historical-reports.svg?branch=master)](https://travis-ci.org/Netflix-Skunkworks/historical-reports) 
[![Coverage Status](https://coveralls.io/repos/github/Netflix-Skunkworks/historical-reports/badge.svg)](https://coveralls.io/github/Netflix-Skunkworks/historical-reports)
[![PyPI version](https://badge.fury.io/py/historical-reports.svg)](https://badge.fury.io/py/historical-reports)

## Historical-Reports is under heavy development and is not ready for production use.

This project contains Lambda functions that generate reports based on events produced by [Historical](https://github.com/Netflix-Skunkworks/historical).
These reports collate and transform data stored in the historical DynamoDB tables, and can publish events and notifications to other applications.

This project is organized into sub-directories with independent lambda functions. You may choose to deploy them independently
into your infrastructure.

The following reports are available:
1. S3
 

# Report Functions

## S3 
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
The Deployment docs are currently being re-written. We will have more to announce soon!