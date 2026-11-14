import warnings
import os
import builtins
from multiprocessing.pool import ThreadPool
from threading import Event, get_ident
from types import SimpleNamespace
from packaging.version import Version
import pytest
import unittest
from unittest.mock import MagicMock, patch

import pyspark
from joblib import Parallel, delayed
from pyspark.sql import SparkSession

from joblibspark.backend import SparkDistributedBackend
import joblibspark.backend

joblibspark.backend._DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE = 8


spark_version = os.environ.get("PYSPARK_VERSION", pyspark.__version__)
is_spark_connect_mode = os.environ.get("TEST_SPARK_CONNECT", "false").lower() == "true"


if Version(spark_version).major >= 4:
    spark_connect_jar = ""
else:
    spark_connect_jar = f"org.apache.spark:spark-connect_2.12:{spark_version}"


class TestLocalSparkCluster(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        if is_spark_connect_mode:
            cls.spark = (
                SparkSession.builder.config(
                    "spark.jars.packages", spark_connect_jar
                )
                    .remote("local-cluster[1, 2, 1024]")
                    .appName("Test")
                    .getOrCreate()
            )
        else:
            cls.spark = (
                SparkSession.builder.master("local-cluster[1, 2, 1024]").getOrCreate()
            )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def test_effective_n_jobs(self):
        backend = SparkDistributedBackend()

        assert backend.effective_n_jobs(n_jobs=None) == 1
        assert backend.effective_n_jobs(n_jobs=4) == 4

        if is_spark_connect_mode:
            assert (
                backend.effective_n_jobs(n_jobs=-1) ==
                joblibspark.backend._DEFAULT_N_JOBS_IN_SPARK_CONNECT_MODE
            )
        else:
            max_num_concurrent_tasks = 8
            backend._get_max_num_concurrent_tasks = MagicMock(return_value=max_num_concurrent_tasks)
            assert backend.effective_n_jobs(n_jobs=-1) == 8
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                assert backend.effective_n_jobs(n_jobs=16) == 16
                assert len(w) == 1


class TestBasicSparkCluster(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.num_cpus_per_spark_task = 1
        cls.num_gpus_per_spark_task = 1
        gpu_discovery_script_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "discover_simulated_gpus.sh"
        )

        cls.spark = (
            SparkSession.builder.master("local-cluster[1, 2, 1024]")
            .config("spark.task.cpus", "1")
            .config("spark.task.resource.gpu.amount", "1")
            .config("spark.executor.cores", "2")
            .config("spark.worker.resource.gpu.amount", "2")
            .config("spark.executor.resource.gpu.amount", "2")
            .config("spark.task.maxFailures", "1")
            .config(
                "spark.worker.resource.gpu.discoveryScript", gpu_discovery_script_path
            )
            .getOrCreate()
        )

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    @unittest.skipIf(Version(pyspark.__version__).release < (3, 4, 0),
                     "Resource group is only supported since spark 3.4.0")
    def test_resource_profile(self):
        backend = SparkDistributedBackend(
            num_cpus_per_spark_task=self.num_cpus_per_spark_task,
            num_gpus_per_spark_task=self.num_gpus_per_spark_task)

        assert backend._support_stage_scheduling

        resource_group = backend._resource_profile
        assert resource_group.taskResources['cpus'].amount == 1.0
        assert resource_group.taskResources['gpu'].amount == 1.0

    @unittest.skipIf(Version(pyspark.__version__).release < (3, 4, 0),
                     "Resource group is only supported since spark 3.4.0")
    def test_resource_with_default(self):
        backend = SparkDistributedBackend()

        assert backend._support_stage_scheduling

        resource_group = backend._resource_profile
        assert resource_group.taskResources['cpus'].amount == 1.0


@pytest.mark.parametrize("master,has_resources,expected,warning", [
    ("spark://master:7077", True, True, None),
    ("spark://master:7077", False, False, "Spark version does not support"),
    ("local[1]", True, False, "Spark local mode doesn't support"),
])
def test_stage_scheduling_warns_only_when_unsupported(master, has_resources, expected, warning):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._is_spark_connect_mode = False
    rdd = SimpleNamespace(withResources=lambda profile: profile) if has_resources else object()
    backend._spark_context = SimpleNamespace(master=master, parallelize=lambda _: rdd)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert backend._is_support_stage_scheduling() is expected

    assert len(caught) == (1 if warning else 0)
    if warning:
        assert warning in str(caught[0].message)


@pytest.mark.parametrize("spark_release,connect_mode,pinned_threads,with_session", [
    ("4.0.0", False, True, True),
    ("4.0.0", False, False, False),
    ("4.0.0", True, False, True),
    ("4.3.0", False, True, True),
    ("4.3.0", False, False, False),
    ("4.3.0", True, False, True),
])
def test_submission_inherits_spark_session(
        spark_release, connect_mode, pinned_threads, with_session):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._spark = MagicMock()
    backend._is_spark_connect_mode = connect_mode
    backend._spark_pinned_threads_enabled = pinned_threads
    backend._pool = MagicMock()
    backend._n_jobs = 2
    backend._is_running = True
    callback = MagicMock()
    wrapped = MagicMock(return_value=42)

    with patch("pyspark.inheritable_thread_target") as inherit, \
            patch("pyspark.__version__", spark_release):
        if with_session:
            inherit.return_value.return_value = wrapped
        else:
            inherit.return_value = wrapped
        backend.apply_async(lambda: 42, callback=callback)

    if with_session:
        inherit.assert_called_once_with(backend._spark)
        inherit.return_value.assert_called_once()
    else:
        inherit.assert_called_once()
        assert callable(inherit.call_args.args[0])
        assert inherit.call_args.args[0] is not backend._spark

    backend._pool.apply_async.assert_called_once()
    assert backend._pool.apply_async.call_args.kwargs == {
        "callback": callback, "error_callback": callback
    }
    submitted = backend._pool.apply_async.call_args.args[0]
    assert submitted() == 42
    wrapped.assert_called_once_with()


def test_submit_uses_async_result():
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend.apply_async = MagicMock()
    func = lambda: 42
    callback = MagicMock()

    result = backend.submit(func, callback=callback)

    backend.apply_async.assert_called_once_with(func, callback=callback)
    assert result is backend.apply_async.return_value


@pytest.mark.parametrize("spark_release,propagate_active", [
    ("4.0.0", True), ("4.0.1", True), ("4.1.0", False), ("4.3.0", False),
])
def test_connect_submission_propagates_active_session(spark_release, propagate_active):
    pytest.importorskip("pandas")
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._spark = MagicMock()
    backend._is_spark_connect_mode = True
    backend._is_running = True
    backend._support_stage_scheduling = False
    backend._spark_supports_job_cancelling = False
    events = []
    caller_thread = get_ident()
    spark_df = MagicMock()
    callback = MagicMock()
    func = MagicMock(return_value=42)

    def inherit(session):
        assert session is backend._spark

        def decorate(target):
            def wrapped():
                events.append(("inherit", get_ident()))
                return target()
            return wrapped
        return decorate

    def activate(session):
        assert session is backend._spark
        events.append(("active", get_ident()))

    def spark_range(*_args, **_kwargs):
        events.append(("range", get_ident()))
        return spark_df

    def map_in_pandas(mapper, **_kwargs):
        frame = next(mapper(iter(())))
        return SimpleNamespace(
            collect=lambda: [SimpleNamespace(result=frame.iloc[0, 0])]
        )

    backend._spark._set_default_and_active_session.side_effect = activate
    backend._spark.range.side_effect = spark_range
    spark_df.mapInPandas.side_effect = map_in_pandas

    with ThreadPool(1) as pool, \
            patch("pyspark.inheritable_thread_target", side_effect=inherit), \
            patch("pyspark.__version__", spark_release):
        backend._pool = pool
        assert backend.submit(func, callback=callback).get(timeout=5) == 42

    expected_events = ["inherit", "active", "range"] if propagate_active else ["inherit", "range"]
    assert [name for name, _ in events] == expected_events
    assert len({thread for _, thread in events}) == 1
    assert events[0][1] != caller_thread
    func.assert_called_once_with()
    callback.assert_called_once_with(42)


@pytest.mark.parametrize("failure,expected_error,message", [
    (ValueError("batch failed"), ValueError, "batch failed"),
    (SystemExit("driver interrupted"), RuntimeError, "Spark batch was interrupted"),
])
def test_submission_propagates_errors(failure, expected_error, message):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._spark = MagicMock()
    backend._spark.range.side_effect = failure
    backend._is_spark_connect_mode = True
    backend._is_running = True
    callback = MagicMock()

    with ThreadPool(1) as pool, \
            patch("pyspark.inheritable_thread_target", return_value=lambda func: func), \
            patch("pyspark.__version__", "4.3.0"):
        backend._pool = pool
        result = backend.submit(lambda: 42, callback=callback)
        with pytest.raises(expected_error, match=message) as error:
            result.get(timeout=5)

    callback.assert_called_once_with(error.value)
    if isinstance(failure, SystemExit):
        assert error.value.__cause__ is failure


def test_submission_without_pinned_threads():
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._spark = SimpleNamespace(sparkContext=MagicMock())
    backend._is_spark_connect_mode = False
    backend._spark_pinned_threads_enabled = False
    backend._is_running = True
    backend._resource_profile = None
    backend._spark_supports_job_cancelling = False
    backend._spark.sparkContext.parallelize.return_value.map.return_value.collect.return_value = [
        pyspark.cloudpickle.dumps(42)
    ]

    with ThreadPool(1) as pool, \
            patch("pyspark.SparkContext._gateway", None), \
            patch("pyspark.sql.is_remote", return_value=False, create=True):
        backend._pool = pool
        assert backend.submit(lambda: 42).get(timeout=5) == 42


@pytest.mark.parametrize("pinned_threads", [False, True])
def test_classic_submission_sets_job_group_and_resources(pinned_threads):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    context = MagicMock()
    backend._spark = SimpleNamespace(sparkContext=context)
    backend._is_spark_connect_mode = False
    backend._spark_pinned_threads_enabled = pinned_threads
    backend._is_running = True
    backend._resource_profile = object()
    backend._spark_supports_job_cancelling = True
    backend._job_group = "test-job-group"
    worker_rdd = context.parallelize.return_value.withResources.return_value
    mapped_rdd = MagicMock()
    func = MagicMock(return_value=42)

    def map_task(mapper):
        serialized = [mapper(0)]
        mapped_rdd.collect.return_value = serialized
        mapped_rdd.collectWithJobGroup.return_value = serialized
        return mapped_rdd

    worker_rdd.map.side_effect = map_task
    inherit = (lambda session: lambda target: target) if pinned_threads else (lambda target: target)
    with ThreadPool(1) as pool, \
            patch("pyspark.inheritable_thread_target", side_effect=inherit):
        backend._pool = pool
        assert backend.submit(func).get(timeout=5) == 42

    func.assert_called_once_with()
    context.parallelize.return_value.withResources.assert_called_once_with(
        backend._resource_profile)
    if pinned_threads:
        context.setLocalProperty.assert_any_call("spark.jobGroup.id", backend._job_group)
        context.setLocalProperty.assert_any_call("spark.job.description", "joblib spark jobs")
        mapped_rdd.collect.assert_called_once_with()
        mapped_rdd.collectWithJobGroup.assert_not_called()
    else:
        context.setLocalProperty.assert_not_called()
        mapped_rdd.collectWithJobGroup.assert_called_once_with(
            backend._job_group, "joblib spark jobs")
        mapped_rdd.collect.assert_not_called()


@pytest.mark.parametrize("config_attribute", ["_backend_args", "_backend_kwargs"])
def test_abort_reconfigures_managed_parallel(config_attribute):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend.parallel = SimpleNamespace(n_jobs=2)
    setattr(backend.parallel, config_attribute, {"verbose": 1})
    backend._cancel_all_jobs = MagicMock()
    backend.configure = MagicMock()
    pool = MagicMock()
    backend._pool = pool

    backend.abort_everything(ensure_ready=True)

    backend._cancel_all_jobs.assert_called_once_with()
    pool.terminate.assert_called_once_with()
    assert backend._pool is None
    backend.configure.assert_called_once_with(
        n_jobs=2, parallel=backend.parallel, verbose=1
    )


def test_managed_parallel_reuse_discards_prefetched_batches(request):
    backend = SparkDistributedBackend.__new__(SparkDistributedBackend)
    backend._pool = None
    backend._ipython = None
    backend._spark = SimpleNamespace(sparkContext=MagicMock())
    backend._is_spark_connect_mode = False
    backend._spark_pinned_threads_enabled = False
    backend._resource_profile = None
    backend._spark_supports_job_cancelling = False
    backend.effective_n_jobs = lambda n_jobs: n_jobs
    retrieving = Event()
    aborting = Event()
    backend._cancel_all_jobs = MagicMock(side_effect=aborting.set)
    request.addfinalizer(lambda: backend._pool.terminate() if backend._pool else None)

    backend._spark.sparkContext.parallelize.return_value.map.side_effect = (
        lambda mapper: SimpleNamespace(collect=lambda: [mapper(0)])
    )

    def fail():
        assert retrieving.wait(5)
        raise ValueError("first batch failed")

    def wait_for_abort():
        assert aborting.wait(5)
        return -1

    def retrieve(result, timeout=None):
        retrieving.set()
        return result.get(timeout=timeout)

    with patch("pyspark.inheritable_thread_target", side_effect=lambda target: target), \
            patch.object(backend, "retrieve_result", side_effect=retrieve), \
            Parallel(backend=backend, n_jobs=2, batch_size=1, pre_dispatch=1) as parallel:
        with pytest.raises(ValueError, match="first batch failed"):
            parallel([delayed(fail)(), delayed(wait_for_abort)(), delayed(int)(-2)])
        assert parallel(delayed(int)(value) for value in [1, 2]) == [1, 2]


@pytest.mark.parametrize("missing_module", ["pyspark", "py4j", "packaging"])
def test_registration_preserves_import_failure(missing_module):
    missing = ModuleNotFoundError(f"No module named '{missing_module}'", name=missing_module)
    original_import = builtins.__import__

    def import_backend(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "backend" and level == 1:
            raise missing
        return original_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=import_backend):
        with pytest.raises(ImportError) as error:
            joblibspark.register_spark()

    if missing_module in ("pyspark", "py4j"):
        assert error.value.__cause__ is missing
        assert "standalone" in str(error.value).lower()
        assert "managed" in str(error.value).lower()
    else:
        assert error.value is missing
