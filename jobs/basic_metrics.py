from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext


# Import dependencies ZIP
# sc.addPyFile('src/load.py')


execfile("./__pyfiles__/load.py")

from pyspark.sql.types import *
from pyspark import SQLContext
import json

if __name__ == "__main__":

    import load

    _, df = load_data(sc, filter=[2015])

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    df.registerTempTable("comments")

    dataset_metrics = spark.sql("""
    SELECT
        COUNT(*) AS count_of_comments,
    FROM comments
    """)

    dataset_metrics.write.mode('overwrite').parquet('basic_metrics.parquet')

# """
#         COUNT(DISTINCT author) AS count_of_users,
#         COUNT(DISTINCT link_id) AS count_of_posts,
#         COUNT(DISTINCT subreddit_id) AS count_of_subreddits,
#         MIN(created) AS first_date,
#         MAX(created) AS last_date,
#         SUM(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS count_top,
#         SUM(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS count_low,
#         SUM(score) AS total_score,
#         AVG(score) AS avg_score,
#         MAX(score) AS max_score,
#         MIN(score) AS min_score,
#         SUM(controversiality) AS count_of_controversial,
#         AVG(controversiality) AS prop_of_controversial,
#         SUM(gilded) AS total_gold,
#         AVG(gilded) AS prop_of_gold,
#         SUM(CASE WHEN body=='[deleted]' THEN 1 ELSE 0 END) AS count_of_deleted,
#         SUM(CASE WHEN body=='[removed]' THEN 1 ELSE 0 END) AS count_of_removed,
#         """