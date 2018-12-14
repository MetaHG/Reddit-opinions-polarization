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

# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

def compute_nltk_polarity(msg_body):
    nltk.data.path.append("./nltk_data.zip/nltk_data")
    sid = SentimentIntensityAnalyzer()
    msg_body = sid.polarity_scores(msg_body)
    return msg_body

compute_nltk_polarity_udf = func.udf(compute_nltk_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_nltk_polarity', compute_nltk_polarity_udf)

def compute_blob_polarity(msg_body):
    sentiment = TextBlob(msg_body).sentiment
    return {'polarity': sentiment.polarity, 'subjectivity': sentiment.subjectivity}

compute_blob_polarity_udf = func.udf(compute_blob_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_blob_polarity', compute_blob_polarity_udf)

def count_matches(msg_grams, ref_grams_counter, ref_grams_intensity=None):
    msg_grams_joined = [' '.join(msg_gram) for msg_gram in msg_grams]
    print(msg_grams_joined)
    msg_grams_counter = Counter(msg_grams_joined)
    res_counter = msg_grams_counter & ref_grams_counter
    
    if ref_grams_intensity is not None:
        res_intensity = dict()
        for w, occ in res_counter.items():
            res_intensity[w] = occ * ref_grams_intensity[w]
    
    count = sum(res_counter.values())
    if ref_grams_intensity is None:
        return count
    else:
        intensity = sum(res_intensity.values())
        return {'count':float(count), 'intensity':intensity}
    
def df_count_matches(gram_counter, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter), IntegerType())
    spark.udf.register(sql_fun_name, udf)

def df_count_matches_intensity(gram_counter, intensity_dict, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter, intensity_dict), MapType(StringType(), FloatType()))
    spark.udf.register(sql_fun_name, udf)