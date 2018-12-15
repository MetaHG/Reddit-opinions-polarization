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
    _, df = load_data(sc, sample=None)

    # df.show()

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    df.registerTempTable("comments")

    monthly_contribs = spark.sql("""
        SELECT AVG(nb_sub) AS avg_nb_sub, month, year FROM (
            SELECT author, YEAR(created) AS year, MONTH(created) AS month, COUNT(DISTINCT subreddit) as nb_sub FROM comments
            WHERE author != '[deleted]' AND author != '[removed]'
            GROUP BY author, YEAR(created), MONTH(created)
        )
        GROUP BY month, year
    """)

    # """
    #         SUM(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS count_of_top_comments,
    #     SUM(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS count_of_child_comments,
    #     AVG(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS prop_of_top_comments,
    #     AVG(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS prop_of_child_comments,
    #
    #     SUM(score) AS total_score,
    #     AVG(score) AS avg_score,
    #
    #     SUM(gilded) AS total_gold,
    #     AVG(gilded) AS prop_of_gold,
    #
    #     SUM(CASE WHEN body='[removed]' THEN 1 ELSE 0 END) AS count_of_removed,
    #     AVG(CASE WHEN body='[removed]' THEN 1 ELSE 0 END) AS prop_of_removed,
    # """

    monthly_contribs.write.mode('overwrite').parquet('monthly_contribs.parquet')
