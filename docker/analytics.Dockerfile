FROM apache/spark:3.5.6

USER root
COPY requirements/spark.txt /tmp/requirements-spark.txt
RUN PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip python3 -c "import pyspark; assert pyspark.__version__ == '3.5.6'"
WORKDIR /workspace
ENV PYTHONPATH=/workspace:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
