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


# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
execfile("./__pyfiles__/load.py")

###
### Other metrics (Vulgarity, hate speech)
###

# Define helper functions
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
        return {'count':count, 'intensity':intensity}
    
def df_count_matches(gram_counter, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter), FloatType())
    spark.udf.register(sql_fun_name, udf)

def df_count_matches_intensity(gram_counter, intensity_dict, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter, intensity_dict), MapType(StringType(), FloatType()))
    spark.udf.register(sql_fun_name, udf)


# Load and preprocess sample data
_, messages = load_data(sc, sample=0.001)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

### Vulgarity
bad_words = spark.read.csv('lexicons/bad_words_lexicon/en.csv', header=True)
bw_gram_rank = bad_words.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('en_bad_words')))

bw_1_grams = {i.en_bad_words: np.inf for i in bw_gram_rank.filter('gram_rank == 1').select('en_bad_words').collect()}


# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

# Compute all metrics
df_count_matches(bw_1_grams, 'bw_count_matches')

nlp_bw = cleaned_messages.selectExpr('id', 'creation_date', 'body', 'process_body(body) as tokens')
nlp_bw = nlp_bw.selectExpr('id', 'creation_date', 'body', "bw_count_matches(tokens) as nb_bw_matches")
nlp_bw = nlp_bw.selectExpr('id', 'creation_date', 'nb_bw_matches')

nlp_bw.registerTempTable("nlp_bw_metrics")

daily_bw_metrics = spark.sql("""
SELECT
    creation_date,
    COUNT(*) AS msg_count,
    SUM(nb_bw_matches) AS sum_nb_bw_matches
FROM nlp_bw_metrics
GROUP BY creation_date
ORDER BY creation_date
""")

daily_bw_metrics.write.mode('overwrite').parquet('nlp_bw_metrics_daily.parquet')