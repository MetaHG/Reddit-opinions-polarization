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

	def condense_post_into_text(dataset):
		'''
		Function whose purpose is to condensate all comments
		of one post (identified by link_id) into one array per post 
		in the resulting dataframe. Transform the data into a format
		that is useful to perform LDA on. 
		'''
		#keeping only what matters
		dataset = dataset.select('link_id', 'body', 'created')
		zeroValue = (list(), datetime.date(year=3016, month=12, day=30))
		combFun = lambda l, r: (l[0] + r[0], min(l[1], r[1]))
		seqFun = lambda prev, curr: (prev[0] + [curr[0]], prev[1] if prev[1] < curr[1] else curr[1])
		#removing post that have been deleted/removed (thus hold no more information)
		filtered = dataset.filter(dataset.body != '[removed]').filter(dataset.body != '[deleted]').filter(dataset.body != '')
		post_and_list_token = filtered.rdd.map(lambda r: (r[0], (r[1], r[2]))).aggregateByKey(zeroValue, seqFun, combFun)    
		return post_and_list_token.map(lambda r: (r[0], r[1][0], r[1][1])).toDF().selectExpr("_1 as post_id", "_2 as text","_3 as created")

	_, df = load_data(sc, filter=[2015, 2016])
	df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
	comments = df.select('link_id','body','created', 'subreddit')
	start_date = datetime.date(year=2015, month=11, day=7)
	end_date = datetime.date(year=2016, month=11, day=8)
	year_b4_election_news_comments = comments.filter(comments.created > start_date).filter(comments.created < end_date).filter(comments.subreddit == 'news')
	condense_post_into_text(year_b4_election_news_comments).write.mode('overwrite').parquet('2016_news_comment.parquet')