#!/bin/bash

BRANDS=("biohealboh" "bringgreen" "roundaround" "idealforman" "careplus" "delightproject" "shingmulnara")
BUCKET="de6-team5-bucket"
TABLE_PREFIX="pb"
TODAY=$(date +%Y%m%d)

for BRAND in "${BRANDS[@]}"
do
    S3_PREFIX="s3://$BUCKET/preprocessed_data/pb/${BRAND}/"
    S3A_PREFIX="s3a://$BUCKET/preprocessed_data/pb/${BRAND}/"
    for suffix in product flag reviewDetail
    do
        for folder in $(aws s3 ls "${S3_PREFIX}" | awk '{print $2}' | grep "_${suffix}/" | grep "${TODAY}")
        do
            spark-submit --jars /opt/bitnami/spark/jars/RedshiftJDBC4-1.2.1.1001.jar \
                /opt/bitnami/spark/jobs/redshift_jdbc.py "${S3A_PREFIX}${folder}" "${TABLE_PREFIX}_${suffix}_table"
        done
    done
done
