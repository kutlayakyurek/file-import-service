# Sample File Import Service Project For AWS Lambda Implemented With Java

Sample file import service project for AWS Lambda implemented with Java

## Introduction

This project aims for implementing simple dockerized Spring Boot application

## How To Run

Just build the jar file with Maven and run it with command below. You can use any text based file with the format specified below. 

# Local
You may use SAM CLI to simulate Lambda environment on local

# AWS
Deploy CDK and put csv file into created S3 bucket

## How To Test

1. Put sample csv file to created S3 bucket
2. Go to DynamoDB console and open created table
3. Check for newly created item with three fields (Id, Content, Date)

### Status

- ![#f03c15](https://placehold.it/15/f03c15/000000?text=+) `Urgent Things to Do`

CI/CD job implementation
  
- ![#c5f015](https://placehold.it/15/c5f015/000000?text=+) `Test Coverage`
  
- ![#1589F0](https://placehold.it/15/1589F0/000000?text=+) `Requests`
