# coding: utf-8

###
### Imports
###
import pyspark
from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Language processing
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

# Language processing with TextBlob
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

# Standard library
from collections import Counter
import os
import datetime as dt

###
###
###


# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
execfile("./__pyfiles__/load.py")

###
### Sentence polarity using NLTK
###

def compute_nltk_polarity(msg_body):
    nltk.data.path.append("./nltk_data.zip/nltk_data")
    sid = SentimentIntensityAnalyzer()
    msg_body = sid.polarity_scores(msg_body)
    return msg_body

compute_nltk_polarity_udf = func.udf(compute_nltk_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_nltk_polarity', compute_nltk_polarity_udf)

# Load and preprocess sample data
_, messages = load_data(sc, sample=0.03)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Filter per subreddit
subreddits_set = set(sc.textFile('subreddits_txt/political_subreddits.txt').collect())
messages.withColumn('subreddit', func.udf(lambda s: s.lower(), StringType())(func.col('subreddit')))
messages = messages.filter(func.col('subreddit').isin(subreddits_set))

# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

nlp_nltk_polartiy = cleaned_messages.selectExpr('subreddit'. 'subreddit_id', 'creation_date', 'body', "compute_nltk_polarity(body) as nltk_scores")
nlp_nltk_polartiy = nlp_nltk_polartiy.selectExpr('subreddit'. 'subreddit_id', 'creation_date', 'nltk_scores.neg as nltk_negativity', 'nltk_scores.neu as nltk_neutrality', 'nltk_scores.pos as nltk_positivity')

nlp_nltk_polartiy.registerTempTable("nlp_nltk_metrics")

nltk_metrics = spark.sql("""
SELECT
    subreddit,
    subreddit_id,
    MIN(creation_date) AS min_date,
    MAX(creation_date) AS max_date,
    COUNT(*) AS msg_count,
    SUM(nltk_negativity) AS sum_nltk_neg,
    SUM(nltk_neutrality) AS sum_nltk_neu,
    SUM(nltk_positivity) AS sum_nltk_pos
FROM nlp_nltk_metrics
GROUP BY subreddit, subreddit_id
""")

nltk_metrics.write.mode('overwrite').parquet(f'nlp_nltk_subreddit_political_0.03_{dt.datetime.now()}.parquet')