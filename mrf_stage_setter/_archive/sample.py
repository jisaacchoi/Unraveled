from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import pandas as pd
import math
import json
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import functions as F
import os

# =========================
# 2. Helper to read NDJSON and show schema
# =========================
def read_ndjson_and_show_schema(
    path: str,
    sample_ratio: float = 1.0,
    show_rows: int = 5,
    truncate: bool = True
):
    """
    Read a newline-delimited JSON (NDJSON) file with Spark and show its schema.
    """
    df = (
        spark.read
        .option("multiLine", "false")      # NDJSON: one JSON object per line
        .option("samplingRatio", sample_ratio)
        .json(path)
    )

    print(f"\n=== Schema for: {path} ===\n")
    df.printSchema()

    if show_rows and show_rows > 0:
        print(f"\n=== Showing {show_rows} example row(s) ===\n")
        df.show(show_rows, truncate=truncate)

    return df


def auto_flatten(df):
    """
    Recursively:
      - explode array<struct> columns
      - flatten struct columns into top-level columns

    Column naming:
      - For struct fields, use ONLY the leaf key (subfield name) as the column name.
      - If that name already exists, append _1, _2, ... to make it unique.
    """
    while True:
        schema = df.schema

        struct_cols = [
            f for f in schema.fields
            if isinstance(f.dataType, StructType)
        ]

        array_struct_cols = [
            f for f in schema.fields
            if isinstance(f.dataType, ArrayType)
            and isinstance(f.dataType.elementType, StructType)
        ]

        if not struct_cols and not array_struct_cols:
            # Nothing left to flatten/explode
            break

        # 1) Explode all array<struct> columns
        for f in array_struct_cols:
            print(f"Exploding array<struct> column: {f.name}")
            df = df.withColumn(f.name, F.explode_outer(F.col(f.name)))

        # 2) Flatten all struct columns
        if struct_cols:
            print("Flattening struct columns:", [f.name for f in struct_cols])

            # Track used names so we don't collide
            used_names = set(df.columns)
            new_cols = []

            for f in df.schema.fields:
                if isinstance(f.dataType, StructType):
                    # Replace struct with its fields
                    for subf in f.dataType.fields:
                        base_name = subf.name              # <-- only last-level key
                        new_name = base_name

                        # Avoid collisions by appending _1, _2, ...
                        i = 1
                        while new_name in used_names:
                            new_name = f"{base_name}_{i}"
                            i += 1

                        used_names.add(new_name)

                        new_cols.append(
                            F.col(f"{f.name}.{subf.name}").alias(new_name)
                        )
                else:
                    # Keep non-struct column as-is
                    new_cols.append(F.col(f.name))

            df = df.select(new_cols)

    return df

def write_parquet(
    df,
    output_dir: str,
    partition_cols: list = None,
    mode: str = "overwrite"
):
    """
    Write a Spark DataFrame to Parquet, with optional partitioning.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The DataFrame to save (df_flat in your case).
    output_dir : str
        Output directory where Spark will create Parquet files.
    partition_cols : list, optional
        List of column names to partition by. If None, no partitioning.
    mode : str, optional
        Write mode: overwrite | append | ignore | errorIfExists

    Example:
        write_parquet(df_flat, "D:/out/parquet", ["billing_code"])
        write_parquet(df_flat, "D:/out/unpartitioned")
    """
    writer = df.write.mode(mode)

    if partition_cols:
        print(f"➡ Partitioning by: {partition_cols}")
        writer = writer.partitionBy(*partition_cols)
    else:
        print("➡ Writing WITHOUT partitioning.")

    writer.parquet(output_dir)
    print(f"✅ Parquet files written to: {output_dir}")

    import os, sys

# 👉 Adjust these three if your folders differ
JAVA_HOME   = "C:/Program Files/Java/jdk-17"
SPARK_HOME  = "C:/spark/spark-4.0.1-bin-hadoop3"
HADOOP_HOME = "C:/spark/hadoop"

# Clear any old values in this process
for k in ["HADOOP_HOME", "hadoop.home.dir"]:
    os.environ.pop(k, None)

os.environ["JAVA_HOME"]   = JAVA_HOME
os.environ["SPARK_HOME"]  = SPARK_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME

os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["PATH"] = (
    JAVA_HOME   + "/bin;" +
    HADOOP_HOME + "/bin;" +
    SPARK_HOME  + "/bin;" +
    os.environ["PATH"]
)

print("JAVA_HOME   =", os.environ["JAVA_HOME"])
print("SPARK_HOME  =", os.environ["SPARK_HOME"])
print("HADOOP_HOME =", os.environ["HADOOP_HOME"])
print("repr(HADOOP_HOME) ->", repr(os.environ["HADOOP_HOME"]))

# =========================
# CONFIG: EDIT THESE PATHS
# =========================
NDJSON_PATH = r"D:\payer_mrf\ndjson\test\2025-11-25\2025-11_051_06E0_in-network-rates_4_of_9.ndjson"
# Spark will write a *directory* if it succeeds
SPARK_PARQUET_DIR = r"D:\payer_mrf\ndjson\test\2025-11-25\test_spark_parquet"
# Pandas will write a single .parquet file if Spark fails
PANDAS_PARQUET_FILE = r"D:\payer_mrf\ndjson\test\2025-11-25\test_pandas_parquet.parquet"


from pyspark.sql import SparkSession

hadoop_home = os.environ["HADOOP_HOME"]
python_exec = os.environ["PYSPARK_PYTHON"]

spark = (
    SparkSession.builder
    .appName("NDJSON Reader/Writer")
    .master("local[*]")
    # Force Hadoop home + disable native libs
    .config(
        "spark.driver.extraJavaOptions",
        f"-Dhadoop.home.dir={hadoop_home} -Dhadoop.native.lib=false"
    )
    .config(
        "spark.executor.extraJavaOptions",
        f"-Dhadoop.home.dir={hadoop_home} -Dhadoop.native.lib=false"
    )
    # Also tell executors exactly which Python to use
    .config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
    .getOrCreate()
)

print("Spark version:", spark.version)

import os
import shutil

JAVA_HOME = r"C:\Program Files\Java\jdk-17"  # <-- adjust if needed

print("JAVA_HOME folder exists? ", os.path.isdir(JAVA_HOME))

java_exe = os.path.join(JAVA_HOME, "bin", "java.exe")
print("java.exe exists?        ", os.path.exists(java_exe))

print("java on PATH?           ", shutil.which("java"))

# =========================
# 3. Read NDJSON and inspect schema
# =========================
df = read_ndjson_and_show_schema(
    path=NDJSON_PATH,
    sample_ratio=0.2,
    show_rows=3,
    truncate=False
)

# If your column is literally named "negotiated rates" with a space, rename it
if "negotiated rates" in df.columns and "negotiated_rates" not in df.columns:
    df = df.withColumnRenamed("negotiated rates", "negotiated_rates")

print("\n=== Extra peek at first 3 rows (original) ===\n")
df.show(3, truncate=False)

df_flat = auto_flatten(df)

print("\n=== Flattened schema ===")
df_flat.printSchema()

print("\n=== Sample flattened rows ===")
df_flat.show(5, truncate=False)

pdf = df_flat.toPandas()
pdf.to_parquet(
    r"D:\payer_mrf\ndjson\test\2025-11-25\df_flat_pandas.parquet",
    engine="pyarrow",
    index=False
)