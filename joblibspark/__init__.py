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
Independently maintained Spark backend for joblib, distributed as joblibspark-fabric.

Fork modifications are documented in CHANGES.rst.
"""
__version__ = '0.7.1'


def register_spark():
    """
    Register spark backend into joblib.
    """
    try:
        from .backend import register # pylint: disable=C0415
        register()
    except ImportError as exc:
        if not isinstance(exc, ModuleNotFoundError) or exc.name not in ("pyspark", "py4j"):
            raise
        msg = (f"Spark dependencies are unavailable: {exc}. "
               "In a standalone environment, install 'joblibspark-fabric[spark]' "
               "or 'joblibspark-fabric[connect]' for Spark Connect. "
               "Do not replace the provided PySpark installation in managed runtimes.")
        raise ImportError(msg) from exc


__all__ = ['register_spark']
