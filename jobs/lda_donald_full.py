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

    params = {
        'start_year': 2016,
        'start_month': 11,
        'start_day': 5,
        'end_year': 2016,
        'end_month': 11,
        'end_day': 7,
        'subreddit': 'The_Donald',
        'n_topics': 10,
        'n_words':10,
        'output_name':'donald2016'

    }

    _, df = load_data(sc, filter=[2015, 2016, 2017])

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    # df.show()

    df.registerTempTable("comments")

    donald_comments = spark.sql("""
    SELECT *
    FROM comments
    WHERE subreddit == 'The_Donald'
    """)

    donald_comments.write.mode('overwrite').parquet('donald_comments.parquet')

    # year_to_filter = range(params['start_year'], params['end_year'])
    #
    # _, df = load_data(sc, filter=year_to_filter)
    # df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
    comments = df.select('link_id','body','created', 'subreddit', 'score')
    # start_date = datetime.date(year=params['start_year'], month=params['start_month'], day=params['start_day'])
    # end_date = datetime.date(year=params['end_year'], month=params['end_month'], day=params['end_day'])
    # comments_filtered = comments.filter(comments.created > start_date).filter(comments.created < end_date).filter(comments.subreddit == params['subreddit'])
    #
    # print "nmb of comm : "+str(comments_filtered.count())
    #
    cleaned_preprocessed = condense_comm_and_preprocessing(comments_filtered, en_stop)
    #
    # #best value for beta is either 0.01 or 0.05
    beta = 0.01
    #
    topics_n_weight, topic_distribution = perform_lda(cleaned_preprocessed, params['n_topics'], params['n_words'], beta, 'text')
    #
    topics_n_weight.write.mode('overwrite').parquet(params['output_name']+'_topic_n_weight.parquet')
    topic_distribution.write.mode('overwrite').parquet(params['output_name']+'_topic_distribution.parquet')