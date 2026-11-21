"""Image build step: put the Spark Kafka connector and its dependencies into pyspark's jars directory.

The connector version is the installed pyspark version, so the two cannot differ. Jobs then need neither
spark.jars.packages nor network access at start. Jars whose artifact pyspark already ships (e.g. snappy-java)
are skipped, so the classpath never holds two versions of one library.
"""

import re
import shutil
import sys
import tempfile
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

CONNECTOR = "spark-sql-kafka-0-10_2.12"


def main() -> None:
    version = pyspark.__version__
    jars_dir = Path(pyspark.__file__).parent / "jars"
    ivy = Path(tempfile.mkdtemp())

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", f"org.apache.spark:{CONNECTOR}:{version}")
        .config("spark.jars.ivy", str(ivy))
        .getOrCreate()
    )
    spark.stop()

    retrieved = {p.name for p in (ivy / "jars").glob("*.jar")}  # "<org>_<module>-<rev>.jar"
    present = [p.name for p in jars_dir.glob("*.jar")]  # "<module>-<rev>.jar"
    copied = []
    # Ivy cache layout: cache/<org>/<module>/jars/<module>-<rev>.jar gives the module name without parsing.
    for jar in sorted((ivy / "cache").glob("*/*/jars/*.jar")):
        org, module = jar.parent.parent.parent.name, jar.parent.parent.name
        if f"{org}_{jar.name}" not in retrieved:
            continue
        if any(re.match(re.escape(module) + r"-\d", name) for name in present):
            print(f"skip {jar.name}: pyspark already ships {module}")
            continue
        shutil.copy(jar, jars_dir / jar.name)
        copied.append(jar.name)
        print(f"copy {jar.name}")

    shutil.rmtree(ivy)
    if f"{CONNECTOR}-{version}.jar" not in copied:
        sys.exit(f"ERROR: {CONNECTOR}-{version}.jar was not copied")


if __name__ == "__main__":
    main()
