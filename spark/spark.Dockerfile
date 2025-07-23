FROM bitnami/spark:3.5.6

USER root

RUN apt-get update && apt-get install -y \
    wget \
    gcc \
    python3-dev \
    --no-install-recommends

# 아래는 직접 파일 다운받아 넣는 걸로 대체
# RUN mkdir -p /opt/bitnami/spark/jars
# RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.4.jar -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
#     && wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/1.12.262/bundle-1.12.262.jar -O /opt/bitnami/spark/jars/aws-sdk-bundle-1.12.262.jar
