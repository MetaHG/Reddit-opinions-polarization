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

if __name__ == "__main__":

	import load

	_, df = load_data(sc, filter=[2016, 2017])
	df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
	comments = df.select('link_id','body','created', 'subreddit', 'score')
	start_date = datetime.date(year=2016, month=1, day=7)
	year_b4_election_news_comments = comments.filter(comments.subreddit == 'hillaryclinton').filter(comments.created > start_date)
	
	year_b4_election_news_comments.write.mode('overwrite').parquet('hillary_comments_sept.parquet')