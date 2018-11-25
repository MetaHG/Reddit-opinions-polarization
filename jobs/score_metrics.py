from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

execfile("./__pyfiles__/load.py")

from pyspark.sql.types import *
from pyspark import SQLContext
import json

if __name__ == "__main__":

    _, df = load_data(sc, sample=None)

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    df.registerTempTable("comments")

    score_metrics = spark.sql("""
        SELECT score, COUNT(*) AS count
        FROM comments
        GROUP BY score
        ORDER BY score
    """)

    score_metrics.write.mode('overwrite').parquet('score_metrics.parquet')
