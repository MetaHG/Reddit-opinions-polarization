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

    daily_metrics = spark.sql("""
    SELECT
    
        *,
    
        AVG(count_of_comments) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_comments_60d_avg,

        AVG(count_of_users) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_users_60d_avg,
        AVG(count_of_posts) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_posts_60d_avg,
        AVG(count_of_subreddits) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_subreddits_60d_avg,

        AVG(count_of_top_comments) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_top_comments_60d_avg,
        AVG(count_of_child_comments) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_child_comments_60d_avg,
        AVG(prop_of_top_comments) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_top_comments_60d_avg,
        AVG(prop_of_child_comments) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_child_comments_60d_avg,

        AVG(total_score) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS total_score_60d_avg,
        AVG(avg_score) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS avg_score_60d_avg,

        AVG(total_gold) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS total_gold_60d_avg,
        AVG(prop_of_gold) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_gold_60d_avg,

        AVG(count_of_removed) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_removed_60d_avg,
        AVG(prop_of_removed) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_removed_60d_avg,
        
        AVG(count_of_deleted) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_deleted_60d_avg,
        AVG(prop_of_deleted) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_deleted_60d_avg,
        
        AVG(count_of_deleted_and_removed) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_deleted_and_removed_60d_avg,
        AVG(prop_of_deleted_and_removed) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_deleted_and_removed_60d_avg,

        AVG(count_of_controversial) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS count_of_controversial_60d_avg,
        AVG(prop_of_controversial) OVER (
            ORDER BY created
            RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
        ) AS prop_of_controversial_60d_avg
    
    FROM (
        SELECT
        
            created,
            COUNT(id) AS count_of_comments,
            
            SUM(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS count_of_top_comments,
            SUM(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS count_of_child_comments,
            AVG(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS prop_of_top_comments,
            AVG(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS prop_of_child_comments,
        
            SUM(score) AS total_score,
            AVG(score) AS avg_score,
        
            SUM(gilded) AS total_gold,
            AVG(gilded) AS prop_of_gold,
        
            SUM(CASE WHEN body='[removed]' THEN 1 ELSE 0 END) AS count_of_removed,
            AVG(CASE WHEN body='[removed]' THEN 1 ELSE 0 END) AS prop_of_removed,
            
            SUM(CASE WHEN body='[deleted]' THEN 1 ELSE 0 END) AS count_of_deleted,
            AVG(CASE WHEN body='[deleted]' THEN 1 ELSE 0 END) AS prop_of_deleted,
            
            SUM(CASE WHEN body='[deleted]' OR body='[removed]' THEN 1 ELSE 0 END) AS count_of_deleted_and_removed,
            AVG(CASE WHEN body='[deleted]' OR body='[removed]' THEN 1 ELSE 0 END) AS prop_of_deleted_and_removed,
        
            COUNT(DISTINCT author) AS count_of_users,
            COUNT(DISTINCT link_id) AS count_of_posts,
            COUNT(DISTINCT subreddit_id) AS count_of_subreddits,
        
            SUM(controversiality) AS count_of_controversial,
            AVG(controversiality) AS prop_of_controversial
            
        FROM comments
        GROUP BY created
        ORDER BY created
    )
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

    # """

    # """

    daily_metrics.write.mode('overwrite').parquet('daily_metrics_full_deleted.parquet')
