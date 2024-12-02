# Pinned to a specific version due to PySpark Pandas requiring distutils which dissappears in python 3.12
# Should be fixed in v4 https://issues.apache.org/jira/browse/SPARK-45390
FROM docker.io/bitnami/spark:3.5.2-debian-12-r0
USER root
RUN install_packages curl
USER 1001
RUN curl https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -o /opt/bitnami/spark/jars/postgresql-42.5.0.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.704/aws-java-sdk-bundle-1.11.704.jar -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.704.jar
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.1/iceberg-spark-runtime-3.5_2.12-1.4.1.jar -o /opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.1.jar
