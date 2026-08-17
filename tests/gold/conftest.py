"""tests/gold/conftest.py
Windows-safe PyTest configuration and Spark fixture for Gold Layer testing.
"""
import os
import sys
from pathlib import Path
import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- 2. WINDOWS JAVA & HADOOP PATH INJECTION ---
JAVA_PATH = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.8-hotspot"
HADOOP_PATH = r"C:\hadoop"

if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_PATH

if "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = HADOOP_PATH

# Append bin folders to PATH so Windows can launch java.exe & winutils.exe
os.environ["PATH"] = (
    os.path.join(JAVA_PATH, "bin") + os.pathsep +
    os.path.join(HADOOP_PATH, "bin") + os.pathsep +
    os.environ.get("PATH", "")
)

# PySpark Python binary binding
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


# --- 3. ISOLATED SPARK SESSION FIXTURE ---
@pytest.fixture(scope="session")
def spark():
    """Builds a single-thread, isolated local SparkSession for Windows testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("GoldUnitTestSuite")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )
    yield session
    session.stop()