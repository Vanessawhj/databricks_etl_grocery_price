from typing import List, Optional, Tuple
from pyspark.sql import SparkSession


def get_excluded_paths(conf_key: str = "pipeline.excluded_paths") -> List[str]:
    """
    Return a list of paths to exclude, driven by a comma-separated Spark conf.
    Example conf: pipeline.excluded_paths="s3://.../file1.csv,s3://.../file2.csv"
    """
    spark = SparkSession.getActiveSession()
    if spark is None:
        return []
    raw = spark.conf.get(conf_key, "")
    return [p.strip() for p in raw.split(",") if p.strip()]
