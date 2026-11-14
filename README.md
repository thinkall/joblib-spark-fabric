# joblibspark-fabric

An independently maintained fork of [joblib-spark](https://github.com/joblib/joblib-spark)
that provides an Apache Spark backend for joblib. Releases are maintained in
[thinkall/joblib-spark-fabric](https://github.com/thinkall/joblib-spark-fabric), independently of upstream.
The distribution name is **`joblibspark-fabric`**; the Python namespace remains
**`joblibspark`**, including `register_spark()` and the `'spark'` backend name.

The upstream Apache-2.0 license and attribution are retained; fork changes are recorded in
[CHANGES.rst](CHANGES.rst).

## Installation

Minimum requirements: Python 3.13, joblib 1.5.0, and PySpark 4.0.
The tested range currently extends through Python 3.14 and PySpark 4.3.0,
for both classic Spark and Spark Connect.

Use mutually compatible versions: Python 3.14 requires PySpark 4.1 or newer
and a joblib release that supports Python 3.14.

**Do not install `joblibspark` and `joblibspark-fabric` together:** both provide
the same import namespace. To migrate an existing environment:

```bash
python -m pip uninstall joblibspark
python -m pip install joblibspark-fabric
```

The base installation deliberately does not install or replace PySpark in managed
environments such as Fabric. If PySpark is not already installed, use an extra:

```bash
python -m pip install "joblibspark-fabric[spark]"
# Spark Connect, including its pandas, Arrow, and gRPC dependencies:
python -m pip install "joblibspark-fabric[connect]"
```

Install `scikit-learn` separately if you use the examples below.

## Examples

Run following example code in `pyspark` shell:

```python
from joblib import parallel_backend
from sklearn.model_selection import cross_val_score
from sklearn import datasets
from sklearn import svm
from joblibspark import register_spark

register_spark() # register spark backend

iris = datasets.load_iris()
clf = svm.SVC(kernel='linear', C=1)
with parallel_backend('spark', n_jobs=3):
  scores = cross_val_score(clf, iris.data, iris.target, cv=5)

print(scores)
```

## Development and releases

```bash
python -m pip install -e ".[test,connect]" scikit-learn build twine
python -m pytest test
python -m pylint joblibspark
python -m build
python -m twine check dist/*
```

CI uses one explicit matrix: Python 3.13 with Spark 4.0-4.3, plus Python 3.14
with Spark 4.2-4.3. The Spark 4.0 baseline pins joblib 1.5.0; other combinations
use a current joblib >=1.5.0. Every combination runs in classic and Connect modes,
without exclusions. Integration tests use Java 21 and local Spark clusters;
the simulated GPU resource tests require Linux.
To run Connect tests, set `TEST_SPARK_CONNECT=true`.

### Containerized cluster tests

An additional CI job runs one standalone master, three worker containers (one
two-core executor per worker), and a separate driver. Connect tests also use
a separate Spark Connect server container. Tests require tasks to rendezvous
across three distinct executor hostnames, check driver filesystem isolation,
perform a cross-worker shuffle, and verify joblib failure recovery.

The suite covers the exported `register_spark()` API and the supported Spark
backend usage surface, rather than only checking that Spark can run tasks:

| API or behavior | Container coverage |
| --- | --- |
| Registration and backend selection | Repeated registration, `Parallel(backend="spark")`, backend instances, `parallel_backend`, and `parallel_config` |
| Job sizing and batching | Explicit/context positive concurrency limits, default `n_jobs`, `-1`, ordered batches, closures, keyword arguments, empty input, and successful reuse |
| Task resource options | `num_cpus_per_spark_task` (1 and 2), `num_gpus_per_spark_task` (0, 1, and 2 simulated GPUs), checked inside executor tasks |
| Nested joblib calls | Sequential execution inside the original executor process, thread, and Spark task |
| Spark context inheritance | Classic local properties and Connect tags reach executor tasks |
| scikit-learn integration | `cross_val_score` with scoring verified on executors |
| Failure handling | Running-sibling cancellation, error propagation, and managed `Parallel` reuse after failure |

Joblib's intentional sequential fallback for `n_jobs=1` (or an unset job count
without a context override) is checked on the driver, not misreported as distributed
execution. Internal backend hooks are exercised through these user calls; this is
not a claim to implement every option of every joblib backend.

Container CI covers Python 3.13 with Spark 4.0.0 and Python 3.14 with Spark 4.2.0
and 4.3.0, in both modes. All containers share one GitHub runner, so this verifies
container/network isolation, not separate physical machines or real GPU computation.
Workers advertise two synthetic GPU addresses solely to test resource scheduling.
Like the existing `TestSimulatedGPUResources` tests, these checks do not require
or test GPU hardware.

With Docker Compose installed, run the classic minimum-runtime case with:

```bash
docker compose -f test/containers/compose.yml build master
docker compose -f test/containers/compose.yml up -d --wait --scale worker=3 master worker
docker compose -f test/containers/compose.yml run --rm --use-aliases driver
docker compose -f test/containers/compose.yml down --volumes --remove-orphans
```

Releases publish to the separate `joblibspark-fabric` project on PyPI using
Trusted Publishing, without an upstream release or a stored API token:

1. Enable GitHub Actions on the fork. Configure a PyPI pending publisher for
   project `joblibspark-fabric`, GitHub
   owner `thinkall`, repository `joblib-spark-fabric`, workflow `release.yml`, and
   environment `pypi`. Configure approval protection for that GitHub environment.
2. Update `joblibspark.__version__` and `CHANGES.rst`.
3. Publish a GitHub release with a matching tag, such as `v0.7.1`.
   The release workflow checks the version, runs CI, builds the distributions,
   and publishes only after the `pypi` environment is approved.

## Limitations

`joblibspark` does not generally support run model inference and feature engineering in parallel.
For example:

```python
from sklearn.feature_extraction import FeatureHasher
h = FeatureHasher(n_features=10)
with parallel_backend('spark', n_jobs=3):
    # This won't run parallelly on spark, it will still run locally.
    h.transform(...)

from sklearn import linear_model
regr = linear_model.LinearRegression()
regr.fit(X_train, y_train)

with parallel_backend('spark', n_jobs=3):
    # This won't run parallelly on spark, it will still run locally.
    regr.predict(diabetes_X_test)
```

Note: for `sklearn.ensemble.RandomForestClassifier`, there is a `n_jobs` parameter,
that means the algorithm support model training/inference in parallel,
but in its inference implementation, it bind the backend to built-in backends,
so the spark backend not work for this case.
