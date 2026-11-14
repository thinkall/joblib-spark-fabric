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
"""
The joblib spark backend implementation.
"""
import atexit
import logging
import queue
import warnings
from multiprocessing.pool import ThreadPool
import uuid
from typing import Optional
from packaging.version import Version, parse

from joblib.parallel \
    import AutoBatchingMixin, ParallelBackendBase, register_parallel_backend, SequentialBackend

from py4j.clientserver import ClientServer

import pyspark
from pyspark import cloudpickle
from pyspark.util import VersionUtils

from .utils import create_resource_profile, get_spark_session


_logger = logging.getLogger("joblibspark.backend")


def register():
    """
    Register joblib spark backend.
    """
    try:
        import sklearn  # pylint: disable=C0415
        if parse(sklearn.__version__) < Version('0.21'):
            warnings.warn("Your sklearn version is < 0.21, but joblib-spark only support "
                          "sklearn >=0.21 . You can upgrade sklearn to version >= 0.21 to "
                          "make sklearn use spark backend.")
    except ImportError:
        pass
    register_parallel_backend('spark', SparkDistributedBackend)


def is_spark_connect_mode():
    """
    Check if running with spark connect mode.
    """
    try:
        from pyspark.sql.utils import is_remote  # pylint: disable=C0415
        return is_remote()
    except ImportError:
        return False


_DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE = 64


# pylint: disable=too-many-instance-attributes
class SparkDistributedBackend(ParallelBackendBase, AutoBatchingMixin):
    """A ParallelBackend which will execute all batches on spark.

    This backend will launch one spark job for task batch. Multiple spark job will run parallelly.
    The maximum parallelism won't exceed `sparkContext._jsc.sc().maxNumConcurrentTasks()`

    Each task batch will be run inside one spark task on worker node, and will be executed
    by `SequentialBackend`
    """

    def __init__(self,
                 num_cpus_per_spark_task: Optional[int] = None,
                 num_gpus_per_spark_task: Optional[int] = None,
                 **backend_args):
        # pylint: disable=super-with-arguments
        super(SparkDistributedBackend, self).__init__(**backend_args)
        self._pool = None
        self._n_jobs = None
        self._spark = get_spark_session()
        self._job_group = "joblib-spark-job-group-" + str(uuid.uuid4())
        self._is_running = False
        try:
            from IPython import get_ipython  # pylint: disable=import-outside-toplevel
            self._ipython = get_ipython()
        except ImportError:
            self._ipython = None

        self._is_spark_connect_mode = is_spark_connect_mode()
        if self._is_spark_connect_mode:
            if Version(pyspark.__version__).major < 4:
                raise RuntimeError(
                    "Joblib spark does not support Spark Connect with PySpark version < 4."
                )
            self._support_stage_scheduling = True
            self._spark_supports_job_cancelling = True
        else:
            self._spark_context = self._spark.sparkContext
            self._spark_pinned_threads_enabled = isinstance(
                self._spark_context._gateway, ClientServer
            )
            self._spark_supports_job_cancelling = (
                self._spark_pinned_threads_enabled
                or hasattr(self._spark_context.parallelize([1]), "collectWithJobGroup")
            )
            self._support_stage_scheduling = self._is_support_stage_scheduling()

        self._resource_profile = self._create_resource_profile(num_cpus_per_spark_task,
                                                               num_gpus_per_spark_task)

    def _is_support_stage_scheduling(self):
        if self._is_spark_connect_mode:
            return Version(pyspark.__version__).major >= 4

        spark_master = self._spark_context.master
        is_spark_local_mode = spark_master == "local" or spark_master.startswith("local[")
        if is_spark_local_mode:
            support_stage_scheduling = False
            warnings.warn("Spark local mode doesn't support stage-level scheduling.")
        else:
            support_stage_scheduling = hasattr(
                self._spark_context.parallelize([1]), "withResources"
            )
            if not support_stage_scheduling:
                warnings.warn("Spark version does not support stage-level scheduling.")
        return support_stage_scheduling

    def _create_resource_profile(self,
                                 num_cpus_per_spark_task,
                                 num_gpus_per_spark_task) -> Optional[object]:
        resource_profile = None
        if self._support_stage_scheduling:
            self.using_stage_scheduling = True

            if is_spark_connect_mode():
                # In Spark Connect mode, we can't read Spark cluster configures.
                default_cpus_per_task = 1
                default_gpus_per_task = 0
            else:
                default_cpus_per_task = int(self._spark.conf.get("spark.task.cpus", "1"))
                default_gpus_per_task = int(
                    self._spark.conf.get("spark.task.resource.gpu.amount", "0")
                )
            num_cpus_per_spark_task = num_cpus_per_spark_task or default_cpus_per_task
            num_gpus_per_spark_task = num_gpus_per_spark_task or default_gpus_per_task

            resource_profile = create_resource_profile(num_cpus_per_spark_task,
                                                       num_gpus_per_spark_task)

        return resource_profile

    def _cancel_all_jobs(self):
        self._is_running = False
        if not self._spark_supports_job_cancelling:
            if self._is_spark_connect_mode:
                warnings.warn("Spark connect does not support job cancellation API "
                              "for Spark version < 3.5")
            else:
                # Note: There's bug existing in `sparkContext.cancelJobGroup`.
                # See https://issues.apache.org/jira/browse/SPARK-31549
                warnings.warn("For spark version < 3, pyspark cancelling job API has bugs, "
                              "so we could not terminate running spark jobs correctly. "
                              "See https://issues.apache.org/jira/browse/SPARK-31549 for "
                              "reference.")
        else:
            if self._is_spark_connect_mode:
                self._spark.interruptTag(self._job_group)
            else:
                self._spark.sparkContext.cancelJobGroup(self._job_group)

    def effective_n_jobs(self, n_jobs):
        if n_jobs is None:
            n_jobs = 1

        if self._is_spark_connect_mode:
            if n_jobs == 1:
                warnings.warn(
                    "The maximum number of concurrently running jobs is set to 1, "
                    "to increase concurrency, you need to set joblib spark backend "
                    "'n_jobs' param to a greater number."
                )

            if n_jobs == -1:
                n_jobs = _DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE
                # pylint: disable = logging-fstring-interpolation
                _logger.warning(
                    "Joblib sets `n_jobs` to default value "
                    f"{_DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE} in Spark Connect mode."
                )
        else:
            max_num_concurrent_tasks = self._get_max_num_concurrent_tasks()
            if n_jobs == -1:
                # n_jobs=-1 means requesting all available workers
                n_jobs = max_num_concurrent_tasks
            if n_jobs > max_num_concurrent_tasks:
                warnings.warn(
                    f"User-specified n_jobs ({n_jobs}) is greater than the max number of "
                    f"concurrent tasks ({max_num_concurrent_tasks}) this cluster can run now."
                    "If dynamic allocation is enabled for the cluster, you might see more "
                    "executors allocated."
                )
        return n_jobs

    def _get_max_num_concurrent_tasks(self):
        # maxNumConcurrentTasks() is a package private API
        # pylint: disable=W0212
        pyspark_version = VersionUtils.majorMinorVersion(pyspark.__version__)
        spark_context = self._spark.sparkContext._jsc.sc()
        if pyspark_version < (3, 1):
            return spark_context.maxNumConcurrentTasks()
        return spark_context.maxNumConcurrentTasks(
                 spark_context.resourceProfileManager().resourceProfileFromId(0))

    def abort_everything(self, ensure_ready=True):
        self._cancel_all_jobs()
        if self._pool is not None:
            # Stop stale batch callbacks before a managed Parallel instance is reused.
            atexit.unregister(self._pool.close)
            self._pool.terminate()
            self._pool = None
        # Joblib retains prefetched, unsubmitted batches when a call fails.
        self.parallel._ready_batches = queue.Queue()  # pylint: disable=protected-access
        if ensure_ready:
            backend_args = getattr(self.parallel, "_backend_kwargs",
                                   getattr(self.parallel, "_backend_args", {}))
            self.configure(n_jobs=self.parallel.n_jobs, parallel=self.parallel,
                           **backend_args)

    def terminate(self):
        self._cancel_all_jobs()

    def configure(self, n_jobs=1, parallel=None, prefer=None, require=None, **_backend_args):
        n_jobs = self.effective_n_jobs(n_jobs)
        self._n_jobs = n_jobs
        self.parallel = parallel  # pylint: disable=attribute-defined-outside-init
        return n_jobs

    def _get_pool(self):
        """Lazily initialize the thread pool
        The actual pool of worker threads is only initialized at the first
        call to apply_async.
        """
        if self._pool is None:
            self._pool = ThreadPool(self._n_jobs)
            atexit.register(self._pool.close)
        return self._pool

    def start_call(self):
        self._is_running = True
        if self._ipython is not None:
            def on_post_run_cell(result):
                try:
                    if result.error_in_exec is not None:
                        self._cancel_all_jobs()
                finally:
                    self._ipython.events.unregister("post_run_cell", on_post_run_cell)

            self._ipython.events.register("post_run_cell", on_post_run_cell)

    def submit(self, func, callback=None):
        """Submit a batch using joblib's current backend interface."""
        return self.apply_async(func, callback=callback)

    def apply_async(self, func, callback=None):
        # Note the `func` args is a batch here. (BatchedCalls type)
        # See joblib.parallel.Parallel._dispatch

        def run_on_worker_and_fetch_result():
            if not self._is_running:
                raise RuntimeError('The task is canceled due to ipython command canceled.')

            if self._is_spark_connect_mode and Version(pyspark.__version__).release < (4, 1):
                # Spark 4.0's public wrapper propagates tags, but not the active session.
                # pylint: disable=protected-access
                self._spark._set_default_and_active_session(self._spark)

            # TODO: handle possible spark exception here. # pylint: disable=fixme
            if self._is_spark_connect_mode:
                spark_df = self._spark.range(1, numPartitions=1)

                def mapper_fn(iterator):
                    import pandas as pd  # pylint: disable=import-outside-toplevel
                    for _ in iterator:  # consume input data.
                        pass

                    result = cloudpickle.dumps(func())
                    yield pd.DataFrame({"result": [result]})

                if self._spark_supports_job_cancelling:
                    self._spark.addTag(self._job_group)

                if self._support_stage_scheduling:
                    collected = spark_df.mapInPandas(
                        mapper_fn,
                        schema="result binary",
                        profile=self._resource_profile,
                    ).collect()
                else:
                    collected = spark_df.mapInPandas(
                        mapper_fn,
                        schema="result binary",
                    ).collect()

                ser_res = bytes(collected[0].result)
            else:
                worker_rdd = self._spark.sparkContext.parallelize([0], 1)
                if self._resource_profile:
                    worker_rdd = worker_rdd.withResources(self._resource_profile)

                def mapper_fn(_):
                    return cloudpickle.dumps(func())

                if self._spark_supports_job_cancelling:
                    if self._spark_pinned_threads_enabled:
                        self._spark.sparkContext.setLocalProperty(
                            "spark.jobGroup.id",
                            self._job_group
                        )
                        self._spark.sparkContext.setLocalProperty(
                            "spark.job.description",
                            "joblib spark jobs"
                        )
                        rdd = worker_rdd.map(mapper_fn)
                        ser_res = rdd.collect()[0]
                    else:
                        rdd = worker_rdd.map(mapper_fn)
                        ser_res = rdd.collectWithJobGroup(
                            self._job_group,
                            "joblib spark jobs"
                        )[0]
                else:
                    rdd = worker_rdd.map(mapper_fn)
                    ser_res = rdd.collect()[0]

            return cloudpickle.loads(ser_res)

        from pyspark import inheritable_thread_target  # pylint: disable=C0415
        if self._is_spark_connect_mode or (
                Version(pyspark.__version__).major >= 4 and self._spark_pinned_threads_enabled):
            run_on_worker_and_fetch_result = \
                inheritable_thread_target(self._spark)(run_on_worker_and_fetch_result)
        else:
            run_on_worker_and_fetch_result = \
                inheritable_thread_target(run_on_worker_and_fetch_result)

        def run_with_error_capture():
            try:
                return run_on_worker_and_fetch_result()
            except BaseException as exc:  # pylint: disable=broad-exception-caught
                if isinstance(exc, Exception):
                    raise
                # ThreadPool otherwise loses the result when its worker exits.
                raise RuntimeError("Spark batch was interrupted") from exc

        return self._get_pool().apply_async(
            run_with_error_capture,
            callback=callback, error_callback=callback
        )


    def get_nested_backend(self):
        """Backend instance to be used by nested Parallel calls.
           For nested backend, always use `SequentialBackend`
        """
        nesting_level = getattr(self, 'nesting_level', 0) + 1
        return SequentialBackend(nesting_level=nesting_level), None
