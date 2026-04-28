FROM python:3.11-slim-bookworm

# Instalar Java (JRE es suficiente para ejecutar Spark)
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Verificar Java
RUN java -version

# Instalar Apache Spark (precompiled with Hadoop 3 - necesario para FSDataInputStream)
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV SPARK_DIST_CLASSPATH=$SPARK_HOME/jars/*

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Configurar Spark en modo local (sin Hadoop)
RUN cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.master local[*]" >> $SPARK_HOME/conf/spark-defaults.conf

# Copiar e instalar dependencias (scraper + pipeline, sin pywin32)
COPY pipeline/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Instalar browsers para Playwright (necesario para scrapling)
RUN python -m playwright install chromium --with-deps 2>/dev/null || \
    python -m playwright install chromium 2>/dev/null || \
    echo "Playwright browser install skipped (not critical for static scraping)"

# Copiar scripts del pipeline
COPY pipeline/ /app/pipeline/

# Copiar el scraper
COPY scrape-data/ /app/scrape-data/

# Copiar input-data de Hadoop (para que pipeline pueda escribir allí)
RUN mkdir -p /app/apache-hadoop/input-data

# Directorio de output compartido
RUN mkdir -p /pipeline_output

WORKDIR /app/pipeline

CMD ["python", "run_pipeline.py"]