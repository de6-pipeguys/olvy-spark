#!/bin/bash

CATEGOTYS=("skincare" "food" "healthcare" "manscare" "haircare" "suncare" "cleansing")
BUCKET="de6-team5-bucket"
TODAY=$(date +%Y%m%d)

for CATEGOTY in "${CATEGOTYS[@]}"
do
    INPUT_PREFIX="s3://${BUCKET}/raw_data/non_pb/${CATEGOTY}/"
    S3A_INPUT_PREFIX="s3a://${BUCKET}/raw_data/non_pb/${CATEGOTY}/"
    S3A_OUTPUT_PREFIX="s3a://${BUCKET}/preprocessed_data/non_pb/${CATEGOTY}/"
    FILES=$(aws s3 ls "${INPUT_PREFIX}" | awk '{print $4}' | grep "${TODAY}")

    for file in $FILES
    do
        spark-submit /opt/bitnami/spark/jobs/data_preprocessing.py \
            "${S3A_INPUT_PREFIX}${file}" "${S3A_OUTPUT_PREFIX}"
    done
done
