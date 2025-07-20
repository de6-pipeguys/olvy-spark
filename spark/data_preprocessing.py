from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, when, to_timestamp, date_format, udf, explode, expr, concat, lit, split
from pyspark.sql.types import *
import sys
import os
import re

# 명령행 인수 확인
if len(sys.argv) != 3:
    print("Usage: python data_preprocessing.py <input_path> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

# 환경변수에서 AWS 자격증명 가져오기
aws_access_key = os.environ.get('YOUR_AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('YOUR_AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION', 'ap-northeast-2')

# Spark 세션 생성 (S3 연동 설정 포함 + 분산처리 최적화)
spark = SparkSession.builder \
    .appName("ETL_TEST") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.region", aws_region) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.block.size", "134217728") \
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
    .config("spark.hadoop.fs.s3a.threads.max", "20") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
    .getOrCreate()

try:
    # S3에서 JSON 파일 읽기 (분산처리 최적화 옵션 적용)
    print("S3에서 JSON 데이터 읽는 중...")
    print(f"파일 경로: {input_path}")
    
    df = spark.read \
        .option("multiline", "true") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .option("prefersDecimal", "true") \
        .option("allowComments", "true") \
        .option("allowUnquotedFieldNames", "true") \
        .option("allowSingleQuotes", "true") \
        .option("allowNumericLeadingZeros", "true") \
        .option("allowBackslashEscapingAnyCharacter", "true") \
        .option("charset", "UTF-8") \
        .json(input_path)
    
    # 파티션 정보 확인
    print(f"현재 파티션 수: {df.rdd.getNumPartitions()}")
    
    # 데이터 크기에 따른 파티션 재조정 (선택사항)
    optimal_partitions = max(1, df.rdd.getNumPartitions() // 2)
    if df.rdd.getNumPartitions() > optimal_partitions:
        print(f"파티션 수를 {optimal_partitions}로 재조정 중...")
        df = df.coalesce(optimal_partitions)
    
    print(f"읽어온 데이터 건수: {df.count()}")
    print("데이터 스키마:")
    df.printSchema()

except Exception as e:
    print(f"데이터 읽기 오류: {e}")
    spark.stop()
    sys.exit(1)

# 1. 상품명 전처리 함수
def clean_product_name(product_name_col):
    """
    상품명에서 대괄호/중괄호/소괄호 태그([특가], {한정}, (증정) 등)를 제거하는 함수
    """
    return trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    product_name_col,
                    r'\[.*?\]\s*', ''  # 대괄호
                ),
                r'\{.*?\}\s*', ''   # 중괄호
            ),
            r'\(.*?\)\s*', ''      # 소괄호
        )
    )

# 상품명 전처리 적용
df_cleaned = df.withColumn(
    "goodsName", 
    clean_product_name(col("goodsName"))
)

# 추가적인 전처리 작업들
df_processed = df_cleaned \
    .withColumn("goodsName", 
                # 연속된 공백을 하나로 변경
                regexp_replace(col("goodsName"), r'\s+', ' ')) \
    .withColumn("goodsName",
                # 양쪽 공백 제거
                trim(col("goodsName"))) \
    .filter(col("goodsName") != "")

# 2. 용량 문자열에서 용량만 추출
df_processed = df_processed.withColumn(
    "capacity",
    expr(r"""
        aggregate(
            transform(
                regexp_extract_all(
                    CASE 
                        WHEN instr(capacity, '옵션') > 0 THEN 
                            regexp_extract(capacity, '옵션[^옵션]*', 0)
                        ELSE 
                            capacity
                    END,
                    '(\\d+(?:\\.\\d+)?)[ ]*(m[lL]|g)(?:[ ]*\\*[ ]*\\d+)?',
                    0
                ),
                x -> 
                    IF(
                        instr(x, '*') > 0,
                        cast(regexp_extract(x, '(\\d+(?:\\.\\d+)?)', 1) as double) * 
                        cast(regexp_extract(x, '\\*[ ]*(\\d+)', 1) as double),
                        cast(regexp_extract(x, '(\\d+(?:\\.\\d+)?)', 1) as double)
                    )
            ),
            0D,
            (acc, x) -> acc + x
        )
    """).cast("double")
)

# 3. reviewDetail의 gauge에서 % 제거하고 int로 변환하는 함수
def clean_review_detail_gauge(review_detail):
    if not review_detail:
        return []
    result = []
    for item in review_detail:
        new_item = item.asDict() if hasattr(item, "asDict") else dict(item)
        gauge_str = new_item.get("gauge", "")
        try:
            new_item["gauge"] = int(gauge_str.replace("%", "")) if gauge_str else None
        except:
            new_item["gauge"] = None
        result.append(new_item)
    return result

clean_review_detail_gauge_udf = udf(clean_review_detail_gauge, 
    ArrayType(
        StructType([
            StructField("gauge", IntegerType()),
            StructField("type", StringType()),
            StructField("value", StringType())
        ])
    )
)

# 4. createdAt 컬럼의 정규화
# 파일명에서 시간 정보 추출 (예: rank_skincare_result_20250716_093000.json → 093000)
match = re.search(r'(\d{6})(?=\.json$|$)', os.path.basename(input_path))
if match:
    time_str = match.group(1)
    # 시간 문자열을 HH:MM:SS 형태로 변환
    hour = time_str[0:2]
    minute = time_str[2:4]
    second = time_str[4:6]
    created_time = f"{hour}:{minute}:{second}"
else:
    created_time = "00:00:00"  # 기본값

# createdAt 컬럼의 날짜와 파일명에서 추출한 시간으로 정규화
df_processed = df_processed.withColumn(
    "createdAt",
    date_format(
        to_timestamp(
            concat(
                date_format(to_timestamp(col("createdAt"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"),
                lit(f" {created_time}")
            ),
            "yyyy-MM-dd HH:mm:ss"
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# 5. salePrice, originalPrice를 정수형으로 변환 (쉼표 제거 후 cast)
df_processed = df_processed \
    .withColumn("salePrice", regexp_replace(col("salePrice"), ",", "").cast("int")) \
    .withColumn("originalPrice", regexp_replace(col("originalPrice"), ",", "").cast("int"))

# originalPrice가 null 또는 0이면 salePrice 값으로 대체
df_processed = df_processed.withColumn(
    "originalPrice",
    when((col("originalPrice").isNull()) | (col("originalPrice") == 0), col("salePrice")).otherwise(col("originalPrice"))
)

# 6. pctOf1 ~ pctOf5 컬럼을 정수형으로 변환 (% 기호 제거 후 int로 변환)
for col_name in ["pctOf1", "pctOf2", "pctOf3", "pctOf4", "pctOf5"]:
    if col_name in df_processed.columns:
        df_processed = df_processed.withColumn(
            col_name,
            regexp_replace(col(col_name), "%", "").cast("int")
        )

# reviewDetail 컬럼 변환 적용
if "reviewDetail" in df_processed.columns:
    df_processed = df_processed.withColumn(
        "reviewDetail",
        clean_review_detail_gauge_udf(col("reviewDetail"))
    )

# category 컬럼이 없으면 파일명에 따라 category 값 추가
if "category" not in df_processed.columns:
    category_value = None
    input_path_lower = input_path.lower()
    if "skincare" in input_path_lower:
        category_value = "스킨케어"
    elif "manscare" in input_path_lower:
        category_value = "맨즈케어"
    elif "cleansing" in input_path_lower:
        category_value = "클렌징"
    elif "food" in input_path_lower:
        category_value = "푸드"
    elif "haircare" in input_path_lower:
        category_value = "헤어케어"
    elif "suncare" in input_path_lower:
        category_value = "선케어"
    if category_value:
        df_processed = df_processed.withColumn("category", lit(category_value))

# 소스 파일명 추출 (확장자 제거)
source_filename = os.path.splitext(os.path.basename(input_path))[0]

# flagList 테이블 parquet 저장
if "flagList" in df_processed.columns:
    # flagList 컬럼 타입 확인
    flagList_type = [f.dataType for f in df_processed.schema.fields if f.name == "flagList"][0]
    if isinstance(flagList_type, ArrayType):
        # 리스트(배열)인 경우
        flag_df = df_processed \
            .select(
                col("goodsName"),
                col("createdAt"),
                explode(col("flagList")).alias("flagName")
            )
    else:
        # 문자열인 경우 (쉼표로 구분된 경우)
        flag_df = df_processed \
            .withColumn("flagListArr", split(col("flagList"), ",")) \
            .select(
                col("goodsName"),
                col("createdAt"),
                explode(col("flagListArr")).alias("flagName")
            )
    flag_output_path = f"{output_path.rstrip('/')}/pp_{source_filename}_flag"
    flag_df.write.mode("append").parquet(flag_output_path)
    print(f"flagList 정보 테이블이 {flag_output_path}에 parquet 형식으로 추가 저장되었습니다.")

# reviewDetail 테이블 parquet 저장
if "reviewDetail" in df_processed.columns:
    review_detail_df = df_processed \
        .select(
            col("goodsName"),
            col("createdAt"),
            col("category"),
            explode(col("reviewDetail")).alias("reviewDetailItem")
        ) \
        .select(
            col("goodsName"),
            col("createdAt"),
            col("reviewDetailItem.type").alias("type"),
            col("reviewDetailItem.value").alias("value"),
            col("reviewDetailItem.gauge").alias("gauge"),
            col("category")
        )
    review_detail_output_path = f"{output_path.rstrip('/')}/pp_{source_filename}_reviewDetail"
    review_detail_df.write.mode("append").parquet(review_detail_output_path)
    print(f"reviewDetail 정보 테이블이 {review_detail_output_path}에 parquet 형식으로 추가 저장되었습니다.")

# 메인 테이블 parquet 저장
main_cols = [c for c in df_processed.columns if c not in ["flagList", "reviewDetail"]]
df_main = df_processed.select(*main_cols)

# isPB 컬럼을 0과 1로 이루어진 smallint로 변환
df_main = df_main.withColumn(
    "isPB",
    col("isPB").cast("smallint")
)

# isSoldout 컬럼도 0과 1로 이루어진 smallint로 변환
df_main = df_main.withColumn(
    "isSoldout",
    col("isSoldout").cast("smallint")
)

main_output_path = f"{output_path.rstrip('/')}/pp_{source_filename}_product"
df_main.write.mode("append").parquet(main_output_path)
print(f"product 테이블 정보가 {main_output_path}에 parquet 형식으로 추가 저장되었습니다.")

# Spark 세션 종료
spark.stop()
