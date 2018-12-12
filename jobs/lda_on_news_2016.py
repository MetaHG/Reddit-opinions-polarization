from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *
from pyspark import SQLContext

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

from nltk.corpus import stopwords
from nltk.corpus import wordnet 
from nltk.stem import WordNetLemmatizer

import nltk

#other words that need to be removed in order to avoid pollution on the topic. (experimental findings on news subreddit)
aux_stop_words = ['people', 'would', 'like']

en_stop = set(stopwords.words('english')+aux_stop_words)
sc = spark.sparkContext

#for both variables to be available on all nodes.
sc.broadcast(en_stop)

# Import dependencies ZIP
# sc.addPyFile('src/load.py')
# Import dependencies ZIP
# sc.addPyFile('src/lda.py')

execfile("./__pyfiles__/load.py")
execfile("./__pyfiles__/lda.py")

from pyspark.sql.types import *
from pyspark import SQLContext
import json
import datetime

if __name__ == "__main__":

    import load
    import lda

    _, df = load_data(sc, filter=[2015, 2016])
    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
    comments = df.select('link_id','body','created', 'subreddit')
    start_date = datetime.date(year=2015, month=12, day=7)
    end_date = datetime.date(year=2016, month=12, day=8)
    year_b4_election_news_comments = comments.filter(comments.created > start_date).filter(comments.created < end_date).filter(comments.subreddit == 'news')
    
    cleaned_preprocessed = condense_comm_and_preprocessing(year_b4_election_news_comments, en_stop)
    
    prev_date = datetime.date(year=2015, month=12, day=7)
    curr_date = datetime.date(year=2016, month=1, day=8)
    
    beta = 0.01    
    res = []
    i = 1
    while i < 12:
        month_comms = cleaned_preprocessed.filter(comments.created > prev_date).filter(comments.created < curr_date)
        topics_w, _ = perform_lda(month_comms, 10, 4,  beta, 'text')
        res.append((curr_date, topics_w))
        i += 1
        prev_date = curr_date
        curr_date = datetime.date(year=2016, month=i, day=8)


    sqlContext = SQLContext(sc)
    res_df = sc.parallelize(res).toDF().selectExpr("_1 as date", "_2 as topics")
    res_df.write.mode('overwrite').parquet('lda_per_month_news_2016.parquet')