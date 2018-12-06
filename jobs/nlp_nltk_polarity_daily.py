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
### Sentence polarity using NLTK
###

def compute_nltk_polarity(msg_body):
    nltk.data.path.append("./nltk_data.zip/nltk_data")
    sid = SentimentIntensityAnalyzer()
    msg_body = sid.polarity_scores(msg_body)
    return msg_body

compute_nltk_polarity_udf = func.udf(compute_nltk_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_nltk_polarity', compute_nltk_polarity_udf)

# # Load and preprocess sample data
_, messages = load_data(sc, sample=0.001)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

nlp_nltk_polartiy = cleaned_messages.selectExpr('id', 'creation_date', 'body', "compute_nltk_polarity(body) as nltk_scores")
nlp_nltk_polartiy = nlp_nltk_polartiy.selectExpr('id', 'creation_date', 'nltk_scores.neg as nltk_negativity', 'nltk_scores.neu as nltk_neutrality', 'nltk_scores.pos as nltk_positivity')

nlp_nltk_polartiy.registerTempTable("nlp_nltk_metrics")

# Compute daily average metrics
daily_nltk_metrics = spark.sql("""
SELECT
    creation_date,
    msg_count,

    AVG(sum_nltk_negativity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_negativity_60d_avg,

    AVG(sum_nltk_neutrality) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_neutrality_60d_avg,
    
    AVG(sum_nltk_positivity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_positivity_60d_avg

FROM (
    SELECT
        creation_date,
        COUNT(*) AS msg_count,
        SUM(nltk_negativity) AS sum_nltk_negativity,
        SUM(nltk_neutrality) AS sum_nltk_neutrality,
        SUM(nltk_positivity) AS sum_nltk_positivity,
    FROM nlp_nltk_metrics
    GROUP BY creation_date
    ORDER BY creation_date
)
""")