#!/bin/bash

CATEGOTYS=("skincare" "food" "healthcare" "manscare" "haircare" "suncare" "cleansing")
BUCKET="de6-team5-bucket"
TABLE_PREFIX="rank"
TODAY=$(date +%Y%m%d)

for CATEGOTY in "${CATEGOTYS[@]}"
do
    S3_PREFIX="s3://$BUCKET/preprocessed_data/non_pb/${CATEGOTY}/"
    S3A_PREFIX="s3a://$BUCKET/preprocessed_data/non_pb/${CATEGOTY}/"
    for suffix in product flag reviewDetail
    do
        for folder in $(aws s3 ls "${S3_PREFIX}" | awk '{print $2}' | grep "_${suffix}/" | grep "${TODAY}")
        do
            spark-submit --jars /opt/bitnami/spark/jars/RedshiftJDBC4-1.2.1.1001.jar \
                /opt/bitnami/spark/jobs/redshift_jdbc.py "${S3A_PREFIX}${folder}" "${TABLE_PREFIX}_${suffix}_table"
        done
    done
done
