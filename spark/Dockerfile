FROM apache/spark:4.1.1 AS runtime

USER root
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar /opt/spark/jars/

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/app
WORKDIR /opt/app
RUN chown spark:spark /opt/app
COPY spark/src/ /opt/app/

USER spark

FROM runtime AS test
USER root
RUN pip install --no-cache-dir pytest==9.1.1
USER spark
COPY tests/spark/ /opt/app/tests/spark/