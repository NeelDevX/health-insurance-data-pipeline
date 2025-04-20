# Use ARM compatible Jupyter base image
FROM jupyter/pyspark-notebook:python-3.9


# Set environment for Java 11
USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-11-openjdk-arm64/bin/java 1 && \
    update-alternatives --set java /usr/lib/jvm/java-11-openjdk-arm64/bin/java && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"


# Define versions
ENV SPARK_VERSION=3.4.4
ENV HADOOP_VERSION=hadoop3
ENV ICEBERG_VERSION=1.3.1
ENV POSTGRES_JDBC_VERSION=42.6.0

# Install Spark
RUN curl -fLo /tmp/spark.tgz https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

RUN pip install pyspark==3.4.4

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"



# Create JAR directory
RUN mkdir -p /opt/spark/jars

# Add Iceberg and PostgreSQL JDBC JARs
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.4_2.12-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-${ICEBERG_VERSION}.jar && \
    curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Configure PySpark JARs to load automatically
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/iceberg-spark-runtime-3.4_2.12-${ICEBERG_VERSION}.jar,/opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar pyspark-shell"

USER $NB_UID
