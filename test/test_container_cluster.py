"""Integration tests for three isolated standalone Spark worker containers."""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

import pytest
from joblib import Parallel, delayed, parallel_backend
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
            .config("spark.cores.max", "3")
            .config("spark.executor.cores", "1")
            .config("spark.executor.memory", "1g")
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


def test_three_worker_hosts_and_shuffle(spark):
    def identify_host(iterator):
        import socket
        for frame in iterator:
            frame["host"] = socket.gethostname()
            yield frame

    rows = (
        spark.range(300, numPartitions=3)
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
    barrier = threading.Barrier(3, timeout=60)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            try:
                barrier.wait()
            except threading.BrokenBarrierError:
                self.send_error(503, "Three executor tasks did not rendezvous")
                return
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

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

    try:
        with parallel_backend("spark"), Parallel(n_jobs=3, batch_size=1, pre_dispatch=3) as parallel:
            results = parallel(
                delayed(run_task)(i, str(private_file), f"http://driver:{server.server_port}/ready")
                for i in range(3)
            )
    finally:
        barrier.abort()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    import joblibspark
    assert [result[0] for result in results] == [1, 2, 3]
    hosts = {result[1] for result in results}
    print(f"Joblib executor hosts: {sorted(hosts)}")
    assert len(hosts) == 3
    assert not hosts.intersection({socket.gethostname(), "master", "connect"})
    assert all(result[2:] == (1, joblibspark.__version__) for result in results)


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

    with parallel_backend("spark"), Parallel(n_jobs=3) as parallel:
        with pytest.raises(Exception, match="distributed batch failed"):
            parallel([delayed(fail)()])
        results = parallel(delayed(square)(i) for i in range(6))
    assert [result[0] for result in results] == [i * i for i in range(6)]
    assert not {result[1] for result in results}.intersection(
        {socket.gethostname(), "master", "connect"})
