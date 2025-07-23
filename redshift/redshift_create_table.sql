-- rank_product_table
DROP TABLE IF EXISTS preprocessed_data.rank_product_table;
CREATE TABLE preprocessed_data.rank_product_table (
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
    detail         VARCHAR(500),
    ingredient     VARCHAR(10000)
);

-- rank_flag_table
DROP TABLE IF EXISTS preprocessed_data.rank_flag_table;
CREATE TABLE preprocessed_data.rank_flag_table (
    goodsName   VARCHAR(500),
    createdAt   TIMESTAMP,
    flagName    VARCHAR(100)
);

-- rank_reviewDetail_table
DROP TABLE IF EXISTS preprocessed_data.rank_reviewDetail_table;
CREATE TABLE preprocessed_data.rank_reviewDetail_table (
    goodsName   VARCHAR(500),
    createdAt   TIMESTAMP,
    type        VARCHAR(50),
    value       VARCHAR(50),
    gauge       INTEGER,
    category    VARCHAR(20)
);

-- pb_product_table
DROP TABLE IF EXISTS preprocessed_data.pb_product_table;
CREATE TABLE preprocessed_data.pb_product_table (
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
    detail         VARCHAR(500),
    ingredients     VARCHAR(10000)
);

-- pb_flag_table
DROP TABLE IF EXISTS preprocessed_data.pb_flag_table;
CREATE TABLE preprocessed_data.pb_flag_table (
    goodsName   VARCHAR(500),
    createdAt   TIMESTAMP,
    flagName    VARCHAR(100)
);

-- pb_reviewDetail_table
DROP TABLE IF EXISTS preprocessed_data.pb_reviewDetail_table;
CREATE TABLE preprocessed_data.pb_reviewDetail_table (
    goodsName   VARCHAR(500),
    createdAt   TIMESTAMP,
    type        VARCHAR(50),
    value       VARCHAR(50),
    gauge       INTEGER,
    category    VARCHAR(20)
);
