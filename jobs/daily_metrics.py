from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
# sc.addPyFile('src/load.py')

from pyspark.sql.types import *
from pyspark import SQLContext
import json

def clean_data(x):
    if type(x['edited']) is bool:
        x['edited'] = -1
    if type(x['edited']) is float:
        x['edited'] = int(x['edited'])

    return x

def load_data(sc, filter=None, sample=None):

    if filter is None:

        rdd = sc.textFile("hdfs:///datasets/reddit_data/*/*")

    elif type(filter) is list:

        directories = ["hdfs:///datasets/reddit_data/{}/*".format(year) for year in filter]
        rdd = sc.textFile(",".join(directories))

    elif type(filter) is dict:

        directories = [["hdfs:///datasets/reddit_data/{}/{}".format(year, month)
                        for month in months] for (year, months) in filter]

        rdd = sc.textFile(",".join(directories))

    else:

        raise NotImplementedError("filter must be either an array, a dictionary or None.")

    if sample is not None:
        rdd = rdd.sample(False, sample, 0)

    schema = StructType([
        StructField("distinguished",    StringType(),   True),
        StructField("retrieved_on",     StringType(),   True),
        StructField("gilded",           IntegerType(),  False),
        StructField("edited",           IntegerType(),  False),
        StructField("id",               StringType(),   False),
        StructField("parent_id",        StringType(),   False),
        StructField("flair_text",       StringType(),   True),
        StructField("author",           StringType(),   True),
        StructField("score",            IntegerType(),  False),
        StructField("ups",              IntegerType(),  True),
        StructField("downs",            IntegerType(),  True),
        StructField("created_utc",      StringType(),   False),
        StructField("author_flair_text", StringType(), True),
        StructField("author_flair_css_class", StringType(), True),
        StructField("author_flair_context", StringType(), True),
        StructField("flair_css_class",  StringType(),   True),
        StructField("subreddit",        StringType(),   False),
        StructField("subreddit_id",     StringType(),   False),
        StructField("score_hidden",     BooleanType(),  True),
        StructField("stickied",         BooleanType(),  True),
        StructField("link_id",          StringType(),   True),
        StructField("controversiality", IntegerType(),  False),
        StructField("body",             StringType(),   False),
        StructField("archived",         BooleanType(),  True)
    ])

    # Create SQL context from SparkContext
    sqlContext = SQLContext(sc)
    rdd = rdd.map(json.loads).map(clean_data)
    df = sqlContext.createDataFrame(rdd, schema)

    return rdd, df


if __name__ == "__main__":

    _, df = load_data(sc, sample=None)

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    df.registerTempTable("comments")

    daily_metrics = spark.sql("""
    SELECT 
        created, 
        count_of_comments,
        
        count_of_users,
        count_of_posts,
        count_of_subreddits,
        
        count_of_top_comments,
        count_of_child_comments,
        prop_of_top_comments,
        prop_of_child_comments,
        
        total_score,
        avg_score,
            
        total_gold,
        prop_of_gold,
                    
        count_of_removed,
        prop_of_removed,
        
        count_of_controversial,
        prop_of_controversial,
        
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
            COUNT(*) AS count_of_comments,
            
            COUNT(DISTINCT author) AS count_of_users,
            COUNT(DISTINCT link_id) AS count_of_posts,
            COUNT(DISTINCT subreddit_id) AS count_of_subreddits,
            
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

            SUM(controversiality) AS count_of_controversial,
            AVG(controversiality) AS prop_of_controversial

        FROM comments
        GROUP BY created
        ORDER BY created
    )
    """)

    daily_metrics.write.mode('overwrite').parquet('daily_metrics.parquet')


