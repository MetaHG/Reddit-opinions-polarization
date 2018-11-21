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

    dataset_metrics = spark.sql("""
    SELECT
        COUNT(*) AS count_of_comments,
        COUNT(DISTINCT author) AS count_of_users,
        COUNT(DISTINCT link_id) AS count_of_posts,
        COUNT(DISTINCT subreddit_id) AS count_of_subreddits,
        MIN(created) AS first_date,
        MAX(created) AS last_date,
        SUM(CASE WHEN parent_id==link_id THEN 1 ELSE 0 END) AS count_top,
        SUM(CASE WHEN parent_id!=link_id THEN 1 ELSE 0 END) AS count_low,
        SUM(score) AS total_score,
        AVG(score) AS avg_score,
        MAX(score) AS max_score,
        MIN(score) AS min_score,
        SUM(controversiality) AS count_of_controversial,
        AVG(controversiality) AS prop_of_controversial,
        SUM(gilded) AS total_gold,
        AVG(gilded) AS prop_of_gold,
        SUM(CASE WHEN body=='[deleted]' THEN 1 ELSE 0 END) AS count_of_deleted,
        SUM(CASE WHEN body=='[removed]' THEN 1 ELSE 0 END) AS count_of_removed,
    FROM comments
    """)

    dataset_metrics.write.mode('overwrite').parquet('basic_metrics.parquet')

