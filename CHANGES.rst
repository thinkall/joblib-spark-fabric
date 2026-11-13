Latest changes
===============

Release 0.7.0 (joblibspark-fabric)
----------------------------------

* Establish an independently released Apache-2.0 fork of joblib-spark 0.6.0.
  Keep the ``joblibspark`` namespace and the ``spark`` backend name unchanged.
* Support Python 3.11-3.14 and compatible PySpark releases from 3.5 to 4.3.0,
  including Spark Connect on PySpark 4.
* Use Spark's public thread-context helper, retaining session propagation for
  Spark Connect 4.0, and provide joblib's current submission entry point.
* Support joblib >1.0 on compatible Python versions and propagate batch failures
  through the thread pool without depending on private joblib traceback wrappers.
  Preserve managed Parallel recovery and report interrupted driver batches.
* Declare runtime dependencies, provide optional Spark installation extras,
  and add independent CI, package validation, and PyPI Trusted Publishing.

Release 0.1.0
--------------
