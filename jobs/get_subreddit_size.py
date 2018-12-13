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
import numpy as np

###
###
###
print(os.listdir('.'))

# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
execfile("./__pyfiles__/load.py")
execfile("./__pyfiles__/preprocess.py")
execfile("./__pyfiles__/nlp_utils.py")

# Load and preprocess sample data
_, messages = load_data(sc)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Filter per subreddit
subreddits_set = set(sc.textFile('subreddits_txt/political_subreddits.txt').collect())
messages.withColumn('subreddit', func.udf(lambda s: s.lower(), StringType())(func.col('subreddit')))
messages = messages.filter(func.col('subreddit').isin(subreddits_set))
print(messages.count())