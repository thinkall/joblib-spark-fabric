# joblibspark-fabric

An independently maintained fork of [joblib-spark](https://github.com/joblib/joblib-spark)
that provides an Apache Spark backend for joblib. Releases are maintained in
[thinkall/joblib-spark-fabric](https://github.com/thinkall/joblib-spark-fabric), independently of upstream.
The distribution name is **`joblibspark-fabric`**; the Python namespace remains
**`joblibspark`**, including `register_spark()` and the `'spark'` backend name.

This is a community fork, not an official Microsoft distribution. The upstream
Apache-2.0 license and attribution are retained; fork changes are recorded in
[CHANGES.rst](CHANGES.rst).

## Installation

Requirements: Python 3.11-3.14, joblib >1.0, and PySpark 3.5 through 4.3.0.
Use a Python version supported by your PySpark release:

| PySpark | Python versions within this fork's support range | Spark Connect |
| --- | --- | --- |
| 3.5.x | 3.11 | No |
| 4.0.x | 3.11-3.13 | Yes |
| 4.1.x, 4.2.x, 4.3.0 | 3.11-3.14 | Yes |

Older joblib releases are tested on older Python versions. Use a current joblib
release on newer Python versions; the dependency range does not imply that every
historical joblib release supports every Python version.

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

CI tests each supported Spark release line on Python 3.11, and Python 3.12-3.14
with Spark 4.3.0, in classic and supported Connect modes. Additional legacy-joblib
jobs cover Spark 3.5 classic and Spark 4.0 Connect. This is representative coverage,
not the full Cartesian product of the compatibility table. Integration tests use Java
17 for Spark 3.5 and Java 21 for Spark 4, and run local Spark clusters;
the simulated GPU resource tests require Linux.
To run Connect tests, set `TEST_SPARK_CONNECT=true`.

Releases publish to the separate `joblibspark-fabric` project on PyPI using
Trusted Publishing, without an upstream release or a stored API token:

1. Enable GitHub Actions on the fork. Configure a PyPI pending publisher for
   project `joblibspark-fabric`, GitHub
   owner `thinkall`, repository `joblib-spark-fabric`, workflow `release.yml`, and
   environment `pypi`. Configure approval protection for that GitHub environment.
2. Update `joblibspark.__version__` and `CHANGES.rst`.
3. Publish a GitHub release with a matching tag, such as `v0.7.0`.
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
