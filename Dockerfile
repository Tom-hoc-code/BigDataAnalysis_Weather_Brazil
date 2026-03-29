FROM ubuntu:22.04

WORKDIR /root

RUN mkdir -p /usr/local/gr2
# ================= INSTALL =================
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    nano && \
    apt-get clean

# ================= INSTALL SPARK =================
# ENV SPARK_VERSION=3.5.1

# RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
#     tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
#     mv spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/gr2/spark && \
#     rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ARG SPARK_VERSION=3.5.1

COPY spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/

RUN set -eux; \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /tmp; \
    mv /tmp/spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/gr2/spark; \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ================= INSTALL PYTHON + FASTAPI =================
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install fastapi uvicorn pyspark

# ================= ENV =================
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME=/usr/local/gr2/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# ================= START =================
CMD ["tail", "-f", "/dev/null"]

COPY start.sh /start.sh
RUN sed -i 's/\r$//' /start.sh && chmod +x /start.sh
RUN chmod +x /start.sh

CMD ["/start.sh"]