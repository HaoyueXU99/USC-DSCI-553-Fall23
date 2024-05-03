# Use an OpenJDK 8 base image
FROM openjdk:8-jdk

RUN apt-get update -q && \
    apt-get install -y -q python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Python, Java, Spark
ENV PYSPARK_PYTHON=python3.9
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=3.2

# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt && \
    mv /opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Install Python packages
RUN pip install --no-cache-dir jupyter pyspark==$SPARK_VERSION

# Expose the Jupyter notebook port
EXPOSE 8888

# Set the working directory
WORKDIR /workspace