"""Integration tests for three isolated standalone Spark worker containers."""
import json
import os
import socket
import threading
import time
from contextlib import contextmanager, ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen
from uuid import uuid4

import pytest
from joblib import Parallel, delayed, parallel_backend, parallel_config
from pyspark.sql import SparkSession

from joblibspark import register_spark


pytestmark = pytest.mark.skipif(
    os.environ.get("TEST_CONTAINER_CLUSTER") != "true",
    reason="Requires the standalone Docker Compose cluster",
)


@pytest.fixture(scope="module")
def spark():
    deadline = time.monotonic() + 120
    workers = []
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urlopen("http://master:8080/json/", timeout=5) as response:
                workers = json.load(response)["workers"]
            last_error = None
        except (OSError, json.JSONDecodeError) as error:
            last_error = repr(error)
        alive = [w for w in workers if w["state"] == "ALIVE"]
        if last_error is None and len(alive) == 3:
            break
        time.sleep(1)
    assert last_error is None and len(alive) == 3, (
        f"Expected three registered workers, found {workers}; last error: {last_error}"
    )
    assert len({w["host"] for w in alive}) == 3

    if os.environ.get("TEST_SPARK_CONNECT") == "true":
        session = SparkSession.builder.remote("sc://connect:15002").getOrCreate()
    else:
        session = (
            SparkSession.builder.master("spark://master:7077")
            .appName("joblibspark-container-integration")
            .config("spark.driver.host", "driver")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.cores.max", "6")
            .config("spark.executor.cores", "2")
            .config("spark.executor.memory", "1g")
            .config("spark.executor.resource.gpu.amount", "2")
            .config("spark.executor.resource.gpu.discoveryScript",
                    "/tests/discover_simulated_gpus.sh")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.task.maxFailures", "1")
            .config("spark.scheduler.minRegisteredResourcesRatio", "1.0")
            .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "120s")
            .getOrCreate()
        )
    register_spark()
    yield session
    session.stop()


@contextmanager
def _driver_server(handler):
    server = ThreadingHTTPServer(("0.0.0.0", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://driver:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_three_worker_hosts_and_shuffle(spark):
    def identify_host(iterator):
        import socket
        for frame in iterator:
            frame["host"] = socket.gethostname()
            yield frame

    rows = (
        spark.range(300, numPartitions=6)
        .mapInPandas(identify_host, "id long, host string", barrier=True)
        .groupBy("host").sum("id").collect()
    )
    hosts = {row.host for row in rows}
    print(f"Spark executor hosts: {sorted(hosts)}")
    assert len(hosts) == 3
    assert not hosts.intersection({socket.gethostname(), "master", "connect"})
    assert sum(row["sum(id)"] for row in rows) == sum(range(300))


def test_joblib_across_isolated_workers(spark, tmp_path):
    private_file = tmp_path / "driver-only"
    private_file.write_text("This file must not be visible to executors.")
    barrier = threading.Barrier(6, timeout=60)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                self.send_error(503, "Six executor tasks did not rendezvous")
                return
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    def run_task(value, driver_file, rendezvous):
        import socket
        from pathlib import Path
        from urllib.request import urlopen
        from pyspark import TaskContext
        import joblibspark

        assert not Path(driver_file).exists(), "Executor shares the driver's filesystem"
        with urlopen(rendezvous, timeout=90) as response:
            assert response.status == 200
        return value + 1, socket.gethostname(), TaskContext.get().cpus(), joblibspark.__version__

    with _driver_server(Handler) as driver_url:
        try:
            with parallel_backend("spark"), Parallel(
                    n_jobs=-1, batch_size=1, pre_dispatch=6) as parallel:
                results = parallel(
                    delayed(run_task)(i, str(private_file), f"{driver_url}/ready")
                    for i in range(6)
                )
        finally:
            barrier.abort()

    import joblibspark
    assert [result[0] for result in results] == list(range(1, 7))
    hosts = {result[1] for result in results}
    print(f"Joblib executor hosts: {sorted(hosts)}")
    assert len(hosts) == 3
    assert not hosts.intersection({socket.gethostname(), "master", "connect"})
    assert all(result[2:] == (1, joblibspark.__version__) for result in results)


@pytest.mark.parametrize("selection", [
    "backend_name", "backend_instance", "parallel_backend", "parallel_config", "all_workers",
])
def test_backend_selection(spark, selection):
    from joblibspark.backend import SparkDistributedBackend

    register_spark()
    register_spark()
    driver_host = socket.gethostname()
    captured = {"offset": 10}

    def transform(value, *, multiplier):
        import socket
        from pyspark import TaskContext

        assert TaskContext.get() is not None
        assert socket.gethostname() != driver_host
        return {"value": value * multiplier + captured["offset"], "payload": b"spark"}

    with ExitStack() as stack:
        options = {"batch_size": 2, "pre_dispatch": 3}
        if selection == "backend_name":
            options.update(backend="spark", n_jobs=3)
        elif selection == "backend_instance":
            options.update(backend=SparkDistributedBackend(), n_jobs=3)
        elif selection == "parallel_backend":
            stack.enter_context(parallel_backend("spark", n_jobs=3))
        elif selection == "parallel_config":
            stack.enter_context(parallel_config(backend="spark", n_jobs=3))
        else:
            stack.enter_context(parallel_backend("spark"))
        parallel = stack.enter_context(Parallel(**options))
        results = parallel(delayed(transform)(value, multiplier=2) for value in range(7))
        assert results == [
            {"value": value * 2 + 10, "payload": b"spark"} for value in range(7)
        ]
        assert parallel([]) == []
        assert parallel([delayed(transform)(9, multiplier=3)]) == [
            {"value": 37, "payload": b"spark"}
        ]


@pytest.mark.parametrize("n_jobs", [None, 1])
def test_single_job_stays_on_driver(spark, n_jobs):
    def identify():
        import socket
        from pyspark import TaskContext
        return socket.gethostname(), TaskContext.get() is None

    assert Parallel(backend="spark", n_jobs=n_jobs)([delayed(identify)()]) == [
        (socket.gethostname(), True)
    ]


@pytest.mark.parametrize("selection", ["explicit", "parallel_backend", "parallel_config"])
def test_positive_job_concurrency_limit(spark, selection):
    barrier = threading.Barrier(3, timeout=60)
    lock = threading.Lock()
    active = 0
    peak = 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                barrier.wait()
                # Keep the first wave occupied while any excess submissions start.
                time.sleep(2)
            except threading.BrokenBarrierError:
                self.send_error(503, "Three concurrent batches were not scheduled")
                return
            finally:
                with lock:
                    active -= 1
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    def occupy_slot(value, driver_url):
        from urllib.request import urlopen
        from pyspark import TaskContext
        assert TaskContext.get() is not None
        with urlopen(driver_url, timeout=90) as response:
            assert response.status == 200
        return value

    with ExitStack() as stack:
        options = {"batch_size": 1, "pre_dispatch": 6}
        if selection == "explicit":
            options.update(backend="spark", n_jobs=3)
        elif selection == "parallel_backend":
            stack.enter_context(parallel_backend("spark", n_jobs=3))
        else:
            stack.enter_context(parallel_config(backend="spark", n_jobs=3))
        driver_url = stack.enter_context(_driver_server(Handler))
        try:
            assert Parallel(**options)(
                delayed(occupy_slot)(value, driver_url) for value in range(6)
            ) == list(range(6))
            assert peak == 3, f"Expected three concurrent batches, observed {peak}"
        finally:
            barrier.abort()


@pytest.mark.parametrize("cpus,gpus", [(1, 0), (2, 0), (1, 1), (2, 2)])
def test_task_resource_requests(spark, cpus, gpus):
    def inspect_resources():
        import socket
        from pyspark import TaskContext

        context = TaskContext.get()
        assert context is not None
        gpu = context.resources().get("gpu")
        return socket.gethostname(), context.cpus(), gpu.addresses if gpu else []

    with parallel_backend("spark", num_cpus_per_spark_task=cpus,
                          num_gpus_per_spark_task=gpus):
        results = Parallel(n_jobs=3, batch_size=1)(
            delayed(inspect_resources)() for _ in range(3))
    for host, actual_cpus, addresses in results:
        assert host not in {socket.gethostname(), "master", "connect"}
        assert actual_cpus == cpus
        assert len(addresses) == gpus
        assert set(addresses).issubset({"0", "1"}), "Only simulated GPUs are configured"


def test_nested_parallel_stays_in_executor(spark):
    def outer(value):
        import os
        import socket
        import threading
        from joblib import Parallel, delayed
        from joblib.parallel import get_active_backend
        from joblib._parallel_backends import SequentialBackend
        from pyspark import TaskContext

        assert isinstance(get_active_backend()[0], SequentialBackend)
        context = TaskContext.get()
        assert context is not None
        identity = (socket.gethostname(), os.getpid(), threading.get_ident(),
                    context.taskAttemptId())

        def inner(offset):
            current = (socket.gethostname(), os.getpid(), threading.get_ident(),
                       TaskContext.get().taskAttemptId())
            assert current == identity
            return value + offset

        return identity[0], Parallel(n_jobs=2)(delayed(inner)(i) for i in range(3))

    with parallel_backend("spark", n_jobs=3):
        results = Parallel()(delayed(outer)(value) for value in range(3))
    assert [result[1] for result in results] == [[value + i for i in range(3)]
                                               for value in range(3)]
    assert all(host != socket.gethostname() for host, _ in results)


def test_caller_context_reaches_executor(spark):
    connect = os.environ.get("TEST_SPARK_CONNECT") == "true"
    marker = f"joblibspark-test-{uuid4().hex}"
    key = "spark.job.tags" if connect else "joblibspark.test.marker"
    if connect:
        spark.addTag(marker)
    else:
        previous = spark.sparkContext.getLocalProperty(key)
        spark.sparkContext.setLocalProperty(key, marker)

    def inspect_context():
        from pyspark import TaskContext
        context = TaskContext.get()
        assert context is not None
        return context.getLocalProperty(key)

    try:
        with parallel_backend("spark", n_jobs=3):
            values = Parallel()(delayed(inspect_context)() for _ in range(3))
        assert all(marker in (value or "") for value in values), values
    finally:
        if connect:
            spark.removeTag(marker)
        else:
            spark.sparkContext.setLocalProperty(key, previous)


def test_sklearn_cross_validation(spark):
    from sklearn.datasets import load_iris
    from sklearn.model_selection import cross_val_score
    from sklearn.svm import SVC

    driver_host = socket.gethostname()

    def score_on_executor(estimator, features, labels):
        import socket
        from pyspark import TaskContext
        assert TaskContext.get() is not None
        assert socket.gethostname() != driver_host
        return estimator.score(features, labels)

    features, labels = load_iris(return_X_y=True)
    with parallel_backend("spark", n_jobs=3):
        scores = cross_val_score(SVC(kernel="linear", C=1), features, labels,
                                 cv=5, scoring=score_on_executor)
    assert scores == pytest.approx([29 / 30, 1, 29 / 30, 29 / 30, 1])


def test_running_sibling_is_cancelled(spark):
    started = threading.Event()
    completed = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/started":
                started.set()
            elif self.path == "/completed":
                completed.set()
            elif self.path == "/wait-started":
                if not started.wait(60):
                    self.send_error(504, "Sibling never started")
                    return
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    def run_task(fail, driver_url):
        import time
        from urllib.request import urlopen
        from pyspark import TaskContext

        assert TaskContext.get() is not None
        endpoint = "/wait-started" if fail else "/started"
        with urlopen(driver_url + endpoint, timeout=90) as response:
            assert response.status == 200
        if fail:
            raise ValueError("cancel running sibling")
        time.sleep(15)
        with urlopen(driver_url + "/completed", timeout=10) as response:
            assert response.status == 200

    with _driver_server(Handler) as driver_url:
        with parallel_backend("spark"), Parallel(
                n_jobs=2, batch_size=1, pre_dispatch=2) as parallel:
            with pytest.raises(Exception, match="cancel running sibling"):
                parallel(delayed(run_task)(fail, driver_url) for fail in [True, False])
        assert started.is_set()
        assert not completed.wait(20), "Running executor task survived cancellation"
    assert spark.range(10).count() == 10


def test_distributed_failure_and_reuse(spark):
    def fail():
        from pyspark import TaskContext
        assert TaskContext.get() is not None, "Failure did not run on an executor"
        raise ValueError("distributed batch failed")

    def square(value):
        import socket
        from pyspark import TaskContext
        assert TaskContext.get() is not None, "Reuse did not run on an executor"
        return value * value, socket.gethostname()

    with parallel_backend("spark"), Parallel(n_jobs=3, batch_size=2, pre_dispatch=3) as parallel:
        assert parallel([]) == []
        with pytest.raises(Exception, match="distributed batch failed"):
            parallel([delayed(fail)()] + [delayed(square)(i) for i in range(8)])
        results = parallel(delayed(square)(i) for i in range(6))
    assert [result[0] for result in results] == [i * i for i in range(6)]
    assert not {result[1] for result in results}.intersection(
        {socket.gethostname(), "master", "connect"})
