ARG PYTHON_VERSION=3.13
FROM eclipse-temurin:21-jre-jammy AS java
FROM python:${PYTHON_VERSION}-slim-bookworm

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV SPARK_LOCAL_DIRS=/tmp/spark
ENV OMP_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
RUN java -version

ARG SPARK_VERSION=4.0.0
ARG JOBLIB_REQUIREMENT=joblib==1.5.0
WORKDIR /src
COPY pyproject.toml setup.py README.md LICENSE CHANGES.rst MANIFEST.in requirements.txt ./
COPY joblibspark ./joblibspark
RUN pip install --no-cache-dir ".[test,connect]" "pyspark[connect]==${SPARK_VERSION}" "${JOBLIB_REQUIREMENT}" scikit-learn \
    && ln -s "$(python -c 'import os,pyspark; print(os.path.dirname(pyspark.__file__))')" /opt/spark

WORKDIR /tests
COPY test/test_container_cluster.py .
COPY test/discover_simulated_gpus.sh .
RUN chmod +x /tests/discover_simulated_gpus.sh
CMD ["python", "-m", "pytest", "test_container_cluster.py", "-v", "-s"]
