from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
execfile('__pyfiles__/load.py')
# execfile('src/load.py')

from pyspark.sql.types import *
from pyspark import SQLContext
import json

if __name__ == "__main__":

    _, df = load_data(sc, filter=[2017], sample=None)

    df.registerTempTable("comments")

    subreddit_agreement = spark.sql("""
    SELECT 
        COUNT(*) AS count,
        SUM(CASE WHEN score > 0 THEN 1 ELSE 0 END) AS count_pos,
        SUM(CASE WHEN score < 0 THEN 1 ELSE 0 END) AS count_neg,
        (SUM(CASE WHEN score > 0 THEN 1 ELSE 0 END) / COUNT(*)) AS agreement_factor
    FROM comments
    WHERE score < -3 OR score > 5
    GROUP BY subreddit
    HAVING COUNT(*) > 100
    """)

    subreddit_agreement.write.mode('overwrite').parquet('subreddit_agreement.parquet')
