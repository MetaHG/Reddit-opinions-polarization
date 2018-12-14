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
execfile("./__pyfiles__/preprocess.py")
execfile("./__pyfiles__/nlp_utils.py")


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

nlp_blob_metrics = cleaned_messages.selectExpr('subreddit', 'subreddit_id', 'creation_date', 'body', "compute_blob_polarity(body) as blob_scores")
nlp_blob_metrics = nlp_blob_metrics.selectExpr('subreddit', 'subreddit_id', 'creation_date', 'blob_scores.polarity as text_blob_polarity', 'blob_scores.subjectivity as text_blob_subjectivity')

nlp_blob_metrics.registerTempTable("nlp_blob_metrics")

blob_metrics = spark.sql("""
SELECT
    subreddit,
    subreddit_id,
    MIN(creation_date) AS min_date,
    MAX(creation_date) AS max_date,
    COUNT(*) AS msg_count,
    SUM(text_blob_polarity) AS sum_blob_polarity,
    SUM(text_blob_subjectivity) AS sum_blob_subjectivity
FROM nlp_blob_metrics
GROUP BY subreddit, subreddit_id
""")


blob_metrics.write.mode('overwrite').parquet('nlp_blob_subreddit_political_0.03.parquet')