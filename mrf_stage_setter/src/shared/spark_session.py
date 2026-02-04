"""Spark utility functions for MRF pipeline."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

LOG = logging.getLogger("src.spark_session")


def setup_spark_environment(
    java_home: str = "C:/Program Files/Java/jdk-17",
    spark_home: str = "C:/spark/spark-4.0.1-bin-hadoop3",
    hadoop_home: str = "C:/spark/hadoop",
) -> None:
    """
    Set up Spark environment variables.
    
    Args:
        java_home: Path to Java installation
        spark_home: Path to Spark installation
        hadoop_home: Path to Hadoop installation
    """
    # Clear any old values in this process
    for k in ["HADOOP_HOME", "hadoop.home.dir"]:
        os.environ.pop(k, None)

    os.environ["JAVA_HOME"] = java_home
    os.environ["SPARK_HOME"] = spark_home
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    os.environ["PATH"] = (
        java_home + "/bin;" +
        hadoop_home + "/bin;" +
        spark_home + "/bin;" +
        os.environ["PATH"]
    )

    LOG.debug("JAVA_HOME = %s", os.environ["JAVA_HOME"])
    LOG.debug("SPARK_HOME = %s", os.environ["SPARK_HOME"])
    LOG.debug("HADOOP_HOME = %s", os.environ["HADOOP_HOME"])


def create_spark_session_simple(app_name: str = "MRF Processor") -> SparkSession:
    """
    Create and configure a SparkSession with simple setup (from sample.py).
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    hadoop_home = os.environ.get("HADOOP_HOME", "C:/spark/hadoop")
    python_exec = os.environ.get("PYSPARK_PYTHON", sys.executable)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.driver.extraJavaOptions",
            f"-Dhadoop.home.dir={hadoop_home} -Dhadoop.native.lib=false"
        )
        .config(
            "spark.executor.extraJavaOptions",
            f"-Dhadoop.home.dir={hadoop_home} -Dhadoop.native.lib=false"
        )
        .config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
        .getOrCreate()
    )

    LOG.info("Spark version: %s", spark.version)
    return spark


def create_spark_session(app_name: str = "MRF Parquet Converter") -> SparkSession:
    """
    Create and configure a SparkSession with full Windows support.
    
    This function provides comprehensive Windows support including:
    - Auto-detection of Hadoop installation
    - winutils.exe and hadoop.dll handling
    - Native library path configuration
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    import tempfile
    
    # Windows-specific: Configure Hadoop settings to avoid winutils.exe requirement
    spark_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        # Suppress Spark's default WARN log level - we use Python logging instead
        # Also disable native IO to avoid Windows hadoop.dll issues
        "spark.driver.extraJavaOptions": "-Dlog4j2.logger.org.apache.spark=ERROR -Dlog4j2.logger.org.apache.hadoop=ERROR -Dio.native.lib.available=false",
        # Memory configuration - increase defaults for large files
        "spark.driver.memory": "4g",  # Driver memory (default is often too low)
        "spark.driver.maxResultSize": "2g",  # Maximum result size driver can collect
        "spark.executor.memory": "4g",  # Executor memory (for local mode, this is the same as driver)
        "spark.sql.shuffle.partitions": "200",  # Increase partitions for better distribution
        "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "64m",  # Target partition size after shuffle
        "spark.memory.fraction": "0.8",  # Fraction of heap used for execution and storage
        "spark.memory.storageFraction": "0.3",  # Fraction of spark.memory.fraction used for storage
        "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",  # Reduce Arrow batch size to save memory
    }
    
    # Windows-specific Hadoop configuration to avoid native library issues
    if sys.platform == "win32":
        # Disable native IO entirely to avoid UnsatisfiedLinkError with hadoop.dll
        # This prevents Hadoop from trying to use native Windows file system operations
        spark_config["spark.hadoop.io.native.lib.available"] = "false"
        # Use manifest-based commit protocol (version 2) - doesn't require native libs
        spark_config["spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version"] = "2"
        spark_config["spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored"] = "true"
        # Use raw local file system (no checksums) to avoid native IO
        spark_config["spark.hadoop.fs.file.impl"] = "org.apache.hadoop.fs.RawLocalFileSystem"
        # Disable file system caching
        spark_config["spark.hadoop.fs.file.impl.disable.cache"] = "true"
        # Also set executor Java options
        spark_config["spark.executor.extraJavaOptions"] = "-Dio.native.lib.available=false"
    
    # Set Python executable for PySpark (fixes "Missing Python executable 'python3'" warning)
    if sys.platform == "win32":
        # On Windows, PySpark looks for 'python3' but it's usually just 'python'
        python_exe = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        LOG.debug("Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to: %s", python_exe)
    
    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME")
        
        # Check common Windows locations if HADOOP_HOME is not set
        if not hadoop_home:
            common_paths = [
                r"C:\spark\hadoop",  # User's location
                r"C:\hadoop",
                r"C:\Program Files\hadoop",
            ]
            for path in common_paths:
                bin_path = os.path.join(path, "bin")
                if os.path.exists(bin_path):
                    hadoop_home = path
                    os.environ["HADOOP_HOME"] = hadoop_home
                    LOG.info("Auto-detected Hadoop installation at: %s", hadoop_home)
                    break
        if not hadoop_home:
            # Create a temporary directory for Hadoop on Windows
            temp_hadoop_dir = os.path.join(tempfile.gettempdir(), "hadoop_spark")
            bin_dir = os.path.join(temp_hadoop_dir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            
            # Check if winutils.exe exists, if not, warn user
            winutils_path = os.path.join(bin_dir, "winutils.exe")
            if not os.path.exists(winutils_path):
                LOG.warning(
                    "winutils.exe not found at %s. PySpark on Windows requires winutils.exe.\n"
                    "Please download winutils.exe from: https://github.com/steveloughran/winutils\n"
                    "Place it in: %s\n"
                    "Or set HADOOP_HOME to a directory containing winutils.exe in its bin/ subdirectory.",
                    winutils_path, bin_dir
                )
            
            # Set environment variable for this process
            os.environ["HADOOP_HOME"] = temp_hadoop_dir
            # Also set Spark config
            spark_config["spark.hadoop.home.dir"] = temp_hadoop_dir
            LOG.debug("Set HADOOP_HOME and spark.hadoop.home.dir to temporary directory: %s", temp_hadoop_dir)
        if hadoop_home:
            # HADOOP_HOME is set or auto-detected, verify files and configure
            # Normalize path to use backslashes on Windows (required for native library loading)
            hadoop_home = os.path.normpath(hadoop_home)
            bin_dir = os.path.normpath(os.path.join(hadoop_home, "bin"))
            winutils_path = os.path.join(bin_dir, "winutils.exe")
            hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")
            
            # Check for required files
            if os.path.exists(hadoop_dll_path):
                LOG.info("Found hadoop.dll at: %s", hadoop_dll_path)
                # Add bin directory to PATH for this process so native libraries can be found
                current_path = os.environ.get("PATH", "")
                if bin_dir not in current_path:
                    os.environ["PATH"] = f"{bin_dir};{current_path}"
                    LOG.debug("Added %s to PATH for native library loading", bin_dir)
            else:
                LOG.warning("hadoop.dll not found at: %s - native library errors may occur", hadoop_dll_path)
            
            if os.path.exists(winutils_path):
                LOG.info("Found winutils.exe at: %s", winutils_path)
            else:
                LOG.warning(
                    "winutils.exe not found at: %s. Some operations may fail.\n"
                    "Download from: https://github.com/steveloughran/winutils",
                    winutils_path
                )
            
            # Configure Spark to use HADOOP_HOME (normalize to backslashes for Windows)
            spark_config["spark.hadoop.home.dir"] = hadoop_home
            # Add Java library path for native libraries (use backslashes)
            java_lib_path = f"-Djava.library.path={bin_dir}"
            # Also try to disable native IO if DLL can't be loaded
            disable_native_io = "-Dhadoop.native.lib=false"
            current_java_opts = spark_config.get("spark.driver.extraJavaOptions", "")
            spark_config["spark.driver.extraJavaOptions"] = f"{current_java_opts} {java_lib_path} {disable_native_io}".strip()
            # Also set for executors
            spark_config["spark.executor.extraJavaOptions"] = f"{java_lib_path} {disable_native_io}".strip()
            LOG.info("Configured Spark to use HADOOP_HOME: %s (with native library path: %s)", hadoop_home, bin_dir)
        else:
            # No HADOOP_HOME found, create temporary directory
            temp_hadoop_dir = os.path.join(tempfile.gettempdir(), "hadoop_spark")
            bin_dir = os.path.join(temp_hadoop_dir, "bin")
            os.makedirs(bin_dir, exist_ok=True)
            
            # Check if winutils.exe exists, if not, warn user
            winutils_path = os.path.join(bin_dir, "winutils.exe")
            if not os.path.exists(winutils_path):
                LOG.warning(
                    "winutils.exe not found at %s. PySpark on Windows requires winutils.exe.\n"
                    "Please download winutils.exe from: https://github.com/steveloughran/winutils\n"
                    "Place it in: %s\n"
                    "Or set HADOOP_HOME to a directory containing winutils.exe in its bin/ subdirectory.",
                    winutils_path, bin_dir
                )
            
            # Set environment variable for this process
            os.environ["HADOOP_HOME"] = temp_hadoop_dir
            # Also set Spark config
            spark_config["spark.hadoop.home.dir"] = temp_hadoop_dir
            LOG.debug("Set HADOOP_HOME and spark.hadoop.home.dir to temporary directory: %s", temp_hadoop_dir)
        
        # Try to configure Spark to work around winutils requirement
        # Use LocalFileSystem which may have fewer permission requirements
        spark_config["spark.hadoop.fs.file.impl"] = "org.apache.hadoop.fs.LocalFileSystem"
    
    spark_builder = SparkSession.builder.appName(app_name)
    for key, value in spark_config.items():
        spark_builder = spark_builder.config(key, value)
    
    spark = spark_builder.getOrCreate()
    LOG.info("Spark version: %s", spark.version)
    return spark

