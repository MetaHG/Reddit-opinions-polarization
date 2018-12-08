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
### Sentence polarity using TextBlob
###

# Using simple sentence polarity analysis
def compute_blob_polarity(msg_body):
    sentiment = TextBlob(msg_body).sentiment
    return {'polarity': sentiment.polarity, 'subjectivity': sentiment.subjectivity}

compute_blob_polarity_udf = func.udf(compute_blob_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_blob_polarity', compute_blob_polarity_udf)


# Load and preprocess sample data
_, messages = load_data(sc, filter=[2015, 2016, 2017], sample=0.1)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

nlp_blob_polartiy = cleaned_messages.selectExpr('id', 'creation_date', 'body', "compute_blob_polarity(body) as blob_scores")
nlp_blob_polartiy = nlp_blob_polartiy.selectExpr('id', 'creation_date', 'blob_scores.polarity as text_blob_polarity', 'blob_scores.subjectivity as text_blob_subjectivity')

nlp_blob_polartiy.registerTempTable("nlp_blob_metrics")

daily_blob_metrics = spark.sql("""
SELECT
    creation_date,
    COUNT(*) AS msg_count,
    SUM(text_blob_polarity) AS sum_blob_polarity,
    SUM(text_blob_subjectivity) AS sum_blob_subjectivity
FROM nlp_blob_metrics
GROUP BY creation_date
ORDER BY creation_date
""")

daily_blob_metrics.write.mode('overwrite').parquet('nlp_blob_metrics_daily_full_15_17_0.1.parquet')