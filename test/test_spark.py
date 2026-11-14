#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import logging
from time import sleep
import pytest
import os
from packaging.version import Version
import unittest

from joblib import Parallel, delayed, parallel_backend

from joblibspark import register_spark
import joblibspark.backend

from pyspark.sql import SparkSession
import pyspark

_logger = logging.getLogger("Test")
_logger.setLevel(logging.INFO)

joblibspark.backend._DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE = 2


spark_version = os.environ.get("PYSPARK_VERSION", pyspark.__version__)

is_spark_connect_mode = os.environ.get("TEST_SPARK_CONNECT", "false").lower() == "true"

if Version(spark_version).major >= 4:
    spark_connect_jar = ""
else:
    spark_connect_jar = f"org.apache.spark:spark-connect_2.12:{spark_version}"

register_spark()


class TestSparkCluster(unittest.TestCase):
    spark = None

    @classmethod
    def setup_class(cls):
        spark_session_builder = (
            SparkSession.builder
                .config("spark.task.cpus", "1")
                .config("spark.task.maxFailures", "1")
        )

        if is_spark_connect_mode:
            _logger.info("Test with spark connect mode.")
            cls.spark = (
                spark_session_builder.config(
                    "spark.jars.packages", spark_connect_jar
                )
                    .remote("local-cluster[1, 2, 1024]")  # Adjust the remote address if necessary
                    .appName("Test")
                    .getOrCreate()
            )
        else:
            cls.spark = (
                spark_session_builder.master("local-cluster[1, 2, 1024]")
                    .getOrCreate()
            )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def test_simple(self):
        def inc(x):
            return x + 1

        def slow_raise_value_error(condition, duration=0.05):
            sleep(duration)
            if condition:
                raise ValueError("condition evaluated to True")

        with parallel_backend('spark') as (ba, _):
            seq = Parallel(n_jobs=2)(delayed(inc)(i) for i in range(2))
            assert seq == [inc(i) for i in range(2)]

            with Parallel(n_jobs=2) as parallel:
                with pytest.raises(Exception, match="condition evaluated to True"):
                    parallel(delayed(slow_raise_value_error)(i == 3) for i in range(10))
                assert parallel(delayed(inc)(i) for i in range(2)) == [1, 2]

    def test_sklearn_cv(self):
        pytest.importorskip("sklearn")
        from sklearn.model_selection import cross_val_score
        from sklearn import datasets
        from sklearn import svm

        iris = datasets.load_iris()
        clf = svm.SVC(kernel='linear', C=1)
        with parallel_backend('spark', n_jobs=3):
            scores = cross_val_score(clf, iris.data, iris.target, cv=5)

        expected = [0.97, 1.0, 0.97, 0.97, 1.0]

        for i in range(5):
            assert(pytest.approx(scores[i], 0.01) == expected[i])

        # test with default n_jobs=-1
        with parallel_backend('spark'):
            scores = cross_val_score(clf, iris.data, iris.target, cv=5)

        for i in range(5):
            assert(pytest.approx(scores[i], 0.01) == expected[i])

    def test_job_cancelling(self):
        from joblib import Parallel, delayed
        import time
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmp_dir:
            started = os.path.join(tmp_dir, "started")
            completed = os.path.join(tmp_dir, "completed")

            def test_fn(x):
                if x == 0:
                    deadline = time.monotonic() + 60
                    while not os.path.exists(started) and time.monotonic() < deadline:
                        time.sleep(0.1)
                    assert os.path.exists(started), "Sibling never started"
                    raise RuntimeError("cancel sibling task")
                with open(started, 'w'):
                    pass
                time.sleep(15)
                with open(completed, 'w'):
                    pass

            with pytest.raises(Exception, match="cancel sibling task"):
                with parallel_backend('spark'):
                    Parallel(n_jobs=2, batch_size=1, pre_dispatch=2)(
                        delayed(test_fn)(i) for i in range(2))

            assert os.path.exists(started)
            time.sleep(20)
            assert not os.path.exists(completed), "Running sibling was not cancelled"


@unittest.skipIf(
    (not is_spark_connect_mode and Version(pyspark.__version__).release < (3, 4, 0)) or
    (is_spark_connect_mode and Version(pyspark.__version__).major < 4),
    "Resource group is only supported since Spark 3.4.0 for legacy Spark mode or "
    "since Spark 4 for Spark Connect mode."
)
class TestSimulatedGPUResources(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        gpu_discovery_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "discover_simulated_gpus.sh"
        )

        spark_session_builder = (
            SparkSession.builder
                .config("spark.task.cpus", "1")
                .config("spark.task.resource.gpu.amount", "1")
                .config("spark.executor.cores", "2")
                .config("spark.worker.resource.gpu.amount", "2")
                .config("spark.executor.resource.gpu.amount", "2")
                .config("spark.task.maxFailures", "1")
                .config(
                    "spark.worker.resource.gpu.discoveryScript", gpu_discovery_script_path
                )
        )

        if is_spark_connect_mode:
            _logger.info("Test with spark connect mode.")
            cls.spark = (
                spark_session_builder.config(
                    "spark.jars.packages", spark_connect_jar
                )
                    .remote("local-cluster[1, 2, 1024]")  # Adjust the remote address if necessary
                    .appName("Test")
                    .getOrCreate()
            )
        else:
            cls.spark = (
                spark_session_builder.master("local-cluster[1, 2, 1024]")
                    .getOrCreate()
            )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def test_resource_group(self):
        def get_spark_context(x):
            from pyspark import TaskContext
            taskcontext = TaskContext.get()
            assert taskcontext.cpus() == 1
            assert len(taskcontext.resources().get("gpu").addresses) == 1
            return TaskContext.get()

        if is_spark_connect_mode:
            with parallel_backend('spark', num_gpus_per_spark_task=1) as (ba, _):
                Parallel(n_jobs=5)(delayed(get_spark_context)(i) for i in range(10))
        else:
            with parallel_backend('spark') as (ba, _):
                Parallel(n_jobs=5)(delayed(get_spark_context)(i) for i in range(10))

    def test_customized_resource_group(self):
        def get_spark_context(x):
            from pyspark import TaskContext
            taskcontext = TaskContext.get()
            assert taskcontext.cpus() == 2
            assert len(taskcontext.resources().get("gpu").addresses) == 2
            return taskcontext.cpus()

        with parallel_backend('spark',
                              num_cpus_per_spark_task=2,
                              num_gpus_per_spark_task=2) as (ba, _):
            Parallel(n_jobs=5)(delayed(get_spark_context)(i) for i in range(10))
