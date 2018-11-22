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
import datetime

start_date = datetime.date(year=2016, month=10, day=7)
end_date = datetime.date(year=2016, month=11, day=7)

if __name__ == "__main__":

    import load

    _, df = load_data(sc, filter=[2016])

    comments = df.select('link_id','body','created', 'subreddit')

    oct_2016_news_comments = comments.filter(comments.created > start_date).filter(comments.created < end_date).filter(comments.subreddit == 'news')

    oct_2016_news_comments.write.mode('overwrite').parquet('oct_2016_news_comment.parquet')