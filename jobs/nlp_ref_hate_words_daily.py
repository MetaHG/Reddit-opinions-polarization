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
execfile("./__pyfiles__/preprocess.py")
execfile("./__pyfiles__/nlp_utils.py")

###
### Other metrics (Vulgarity, hate speech)
###

# Load and preprocess sample data
_, messages = load_data(sc, sample=0.01)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Refined hate words
hw_ref_schema = StructType([StructField('hate_words_ref', StringType(), False), StructField('intensity', FloatType(), False)])
hate_words_ref = spark.read.csv('lexicons/hatespeech_lexicon/refined_ngram_dict.csv', header=True, schema=hw_ref_schema)
hw_ref_gram_rank = hate_words_ref.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('hate_words_ref')))

hw_ref_1_grams = Counter({i.hate_words_ref: np.inf for i in hw_ref_gram_rank.filter('gram_rank == 1').select('hate_words_ref').collect()})
hw_ref_1_intensity = {i.hate_words_ref: i.intensity for i in hw_ref_gram_rank.filter('gram_rank == 1').select('hate_words_ref', 'intensity').collect()}


# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

# Compute all metrics
df_count_matches_intensity(hw_ref_1_grams, hw_ref_1_intensity, 'hw_ref_count_matches')

nlp_hw = cleaned_messages.selectExpr('id', 'creation_date', 'body', 'process_body(body) as tokens')
nlp_hw = nlp_hw.selectExpr('id', 'creation_date', 'body', "hw_ref_count_matches(tokens) as hw_ref_matches")
nlp_hw = nlp_hw.selectExpr('id', 'creation_date', 'hw_ref_matches.count as nb_hw_ref_matches', 'hw_ref_matches.intensity as hw_ref_intensity')

nlp_hw.registerTempTable("nlp_hw_metrics")

daily_hw_metrics = spark.sql("""
SELECT
    creation_date,
    COUNT(*) AS msg_count,
    SUM(nb_hw_ref_matches) AS sum_nb_hw_ref_matches,
    SUM(hw_ref_intensity) AS sum_hw_ref_intensity
FROM nlp_hw_metrics
GROUP BY creation_date
ORDER BY creation_date
""")

daily_hw_metrics.write.mode('overwrite').parquet('nlp_ref_hw_metrics_daily_full_0.01.parquet')