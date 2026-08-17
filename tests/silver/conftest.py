"""Pytest fixtures for Phase 5 Silver testing."""

import os
import sys
from pathlib import Path

# 1. Resolve project root and add to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 2. Configure Windows Java and Hadoop paths
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
HADOOP_PATH = r"C:\hadoop"

if os.path.exists(JAVA_PATH):
    os.environ["JAVA_HOME"] = JAVA_PATH
if os.path.exists(HADOOP_PATH):
    os.environ["HADOOP_HOME"] = HADOOP_PATH

# 3. Inject bin paths to Windows system PATH
java_bin = os.path.join(os.environ.get("JAVA_HOME", ""), "bin")
hadoop_bin = os.path.join(os.environ.get("HADOOP_HOME", ""), "bin")
os.environ["PATH"] = f"{java_bin};{hadoop_bin};" + os.environ.get("PATH", "")

# 4. Bind Python workers directly to the active venv executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver_tests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()