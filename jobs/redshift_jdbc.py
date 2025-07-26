import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if len(sys.argv) != 3:
    print("Usage: spark-submit redshift_jdbc.py <s3_parquet_path> <table_name>")
    sys.exit(1)

s3_parquet_path = sys.argv[1]
table_name = sys.argv[2]

redshift_id = os.environ.get('YOUR_REDSHIFT_USER')
redshift_pw = os.environ.get('YOUR_REDSHIFT_PASSWORD')
aws_access_key = os.environ.get('YOUR_AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('YOUR_AWS_SECRET_ACCESS_KEY')

# Redshift 연결 정보
redshift_url = "jdbc:redshift://de6-team5-redshift-cluster.cvkht4jvd430.ap-northeast-2.redshift.amazonaws.com:5439/dev"
redshift_user = redshift_id
redshift_password = redshift_pw
redshift_schema = "preprocessed_data"
redshift_driver = "com.amazon.redshift.jdbc4.Driver"
redshift_table = f"{redshift_schema}.{table_name}"

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Redshift JDBC") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.region", "ap-northeast-2") \
    .getOrCreate()

# S3 parquet 파일 읽기
df = spark.read.parquet(s3_parquet_path)

# isPb, isSoldout 컬럼을 명확하게 int로 변환 (True/False → 1/0)
for col_name in ["isPb", "isSoldout"]:
    if col_name in df.columns:
        df = df.withColumn(col_name, col(col_name).cast("smallint"))

# Redshift에 적재
df.write \
    .format("jdbc") \
    .option("url", redshift_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_user) \
    .option("password", redshift_password) \
    .option("driver", redshift_driver) \
    .option("ssl", "true") \
    .mode("append") \
    .save()

print(f"Redshift에 {s3_parquet_path} 파일 적재 완료!")

spark.stop()

# 사용법 예시
# spark-submit --jars /opt/bitnami/spark/jars/RedshiftJDBC4-1.2.1.1001.jar /opt/bitnami/spark/jobs/redshift_jdbc.py "s3a://de6-team5-bucket/preprocessed_data/non_pb/skincare/pp_rank_skincare_result_20250719_093000_product/" product_table
