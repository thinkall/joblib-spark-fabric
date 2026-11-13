Installing joblibspark-fabric
=============================

``joblibspark-fabric`` is an independently maintained fork of joblib-spark.
The Python import namespace remains ``joblibspark``.

Requirements
------------

Use Python 3.11-3.14 and a compatible PySpark release from 3.5 through 4.3.0.
PySpark 3.5 supports Python 3.11 within this range; PySpark 4.0 supports
Python 3.11-3.13; PySpark 4.1-4.3 supports Python 3.11-3.14.
Spark Connect requires PySpark 4.0 or newer.

Installation
------------

Do not install the original ``joblibspark`` distribution alongside this fork,
because both distributions own the same Python package. Migrate with::

    python -m pip uninstall joblibspark
    python -m pip install joblibspark-fabric

PySpark is not installed or replaced by the base installation, so managed Spark
environments keep their existing runtime. For a standalone environment, use::

    python -m pip install "joblibspark-fabric[spark]"

For Spark Connect and its optional dependencies, use::

    python -m pip install "joblibspark-fabric[connect]"

The existing API is unchanged::

    from joblibspark import register_spark

    register_spark()
