CREATE SCHEMA test_oju;

DROP TABLE IF EXISTS test_oju.flag_table;
CREATE TABLE test_oju.flag_table (
    goodsName   VARCHAR(100),
    createdAt   TIMESTAMP,
    flagName    VARCHAR(100)
);

DROP TABLE IF EXISTS test_oju.reviewDetail_table;
CREATE TABLE test_oju.reviewDetail_table (
    goodsName   VARCHAR(100),
    createdAt   TIMESTAMP,
    type        VARCHAR(20),
    value       VARCHAR(20),
    gauge       INTEGER,
    category    VARCHAR(20)
);

DROP TABLE IF EXISTS test_oju.product_table;
CREATE TABLE test_oju.product_table (
    brandName      VARCHAR(100),
    isPb           SMALLINT,
    goodsName      VARCHAR(500),
    salePrice      INTEGER,
    originalPrice  INTEGER,
    isSoldout      SMALLINT,
    createdAt      TIMESTAMP,
    category       VARCHAR(20),
    rank           INTEGER,
    totalComment   VARCHAR(10),
    numOfReviews   INTEGER,
    avgReview      FLOAT,
    pctOf5         INTEGER,
    pctOf4         INTEGER,
    pctOf3         INTEGER,
    pctOf2         INTEGER,
    pctOf1         INTEGER,
    capacity       VARCHAR(500),
    detail         VARCHAR(200),
    ingredient     VARCHAR(10000)
);
