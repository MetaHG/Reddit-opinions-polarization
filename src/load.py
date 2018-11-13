
import json

from pyspark.sql.types import *
from pyspark import SQLContext

def clean_data(x):
    if type(x['edited']) is bool:
        x['edited'] = -1
    if type(x['edited']) is float:
        x['edited'] = int(x['edited'])

    return x

def load_data(sc, filter=None, sample=True):

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

    if sample:
        rdd = rdd.sample(False, 0.005, 0)

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
        StructField("created_utc",      StringType(),   False),
        StructField("flair_css_class",  StringType(),   True),
        StructField("subreddit",        StringType(),   False),
        StructField("subreddit_id",     StringType(),   False),
        StructField("stickied",         BooleanType(),  True),
        StructField("link_id",          StringType(),   True),
        StructField("controversiality", IntegerType(),  False),
        StructField("body",             StringType(),   False),
    ])

    # Create SQL context from SparkContext
    sqlContext = SQLContext(sc)
    rdd = rdd.map(json.loads).map(clean_data)
    df = sqlContext.createDataFrame(rdd, schema)

    return df


