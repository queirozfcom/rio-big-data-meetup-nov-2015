#!/usr/bin/env bash

aws emr create-cluster \
    --name "1-node dummy cluster" \
    --instance-type m3.xlarge \
    --release-label emr-4.1.0 \
    --instance-count 1 \
    --log-uri s3://rio-big-data-meetup-nov-2015/logs \
    --ec2-attributes KeyName=ubuntu21key \
    --use-default-roles \
    --applications Name=Spark \
    --configurations file://aws-emr/configs/spark.json \
    --auto-terminate