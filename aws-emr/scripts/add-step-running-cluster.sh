#!/usr/bin/env bash

cluster_id=$1
path_to_steps_file=$2

aws emr add-steps \
    --cluster-id $cluster_id \
    --steps file://$path_to_steps_file