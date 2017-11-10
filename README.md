Historical Reports
=====================

## Historical-Reports is under heavy development and is not ready for production use.

This project contains Lambda functions that generate reports based on events produced by [Historical](https://github.com/Netflix-Skunkworks/historical).
These reports collate and transform data stored in the historical DynamoDB tables, and can publish events and notifications to other applications.

This project is organized into sub-directories with independent lambda functions. You may choose to deploy them independently
into your infrastructure.

The following reports are available:
1. S3 - Similar in nature to [SWAG](https://github.com/Netflix-Skunkworks/swag-client), this generates a global dictionary 
of all S3 buckets you have in your account, along with metadata about them. 
This report gets generated into JSON and stored into an S3 bucket(s) of your choosing. The primary
use of this is to know which buckets and which regions and accounts they reside in.
