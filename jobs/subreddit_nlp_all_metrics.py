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
import pickle

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
_, messages = load_data(sc)
messages = messages.withColumn('created_utc', func.from_unixtime(messages['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

# Filter per subreddit
with open('subreddits_set/sports_subreddits.pkl', 'rb') as f:
    subreddits_set = pickle.load(f, encoding='utf-8')
messages.withColumn('subreddit', func.udf(lambda s: s.lower(), StringType())(func.col('subreddit')))
messages = messages.filter(func.col('subreddit').isin(subreddits_set))

# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")

### Vulgarity
bad_words = spark.read.csv('lexicons/bad_words_lexicon/en.csv', header=True)
bw_gram_rank = bad_words.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('en_bad_words')))
bw_1_grams = Counter({i.en_bad_words: np.inf for i in bw_gram_rank.filter('gram_rank == 1').select('en_bad_words').collect()})

### Hate speech
hate_words = spark.read.csv('lexicons/hatespeech_lexicon/hatebase_dict.csv', header=True)
hate_words = hate_words.withColumnRenamed("uncivilised',", 'hate_words').withColumn('hate_words', func.udf(lambda d: d[1:-2])(func.col('hate_words')))
hw_gram_rank = hate_words.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('hate_words')))
hw_1_grams = Counter({i.hate_words: np.inf for i in hw_gram_rank.filter('gram_rank == 1').select('hate_words').collect()})

# Refined hate words
hw_ref_schema = StructType([StructField('hate_words_ref', StringType(), False), StructField('intensity', FloatType(), False)])
hate_words_ref = spark.read.csv('lexicons/hatespeech_lexicon/refined_ngram_dict.csv', header=True, schema=hw_ref_schema)
hw_ref_gram_rank = hate_words_ref.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('hate_words_ref')))
hw_ref_1_grams = Counter({i.hate_words_ref: np.inf for i in hw_ref_gram_rank.filter('gram_rank == 1').select('hate_words_ref').collect()})
hw_ref_1_intensity = {i.hate_words_ref: i.intensity for i in hw_ref_gram_rank.filter('gram_rank == 1').select('hate_words_ref', 'intensity').collect()}

df_count_matches(bw_1_grams, 'bw_count_matches')
df_count_matches(hw_1_grams, 'hw_count_matches')
df_count_matches_intensity(hw_ref_1_grams, hw_ref_1_intensity, 'hw_ref_count_matches')

nlp_metrics_df = cleaned_messages.selectExpr('id', 'subreddit', 'subreddit_id', 'creation_date', 'body', 'process_body(body) as tokens')
nlp_metrics_df = nlp_metrics_df.selectExpr('id', 'subreddit', 'subreddit_id', 'creation_date', 'body', "compute_nltk_polarity(body) as nltk_scores", "compute_blob_polarity(body) as blob_scores", "bw_count_matches(tokens) as nb_bw_matches", "hw_count_matches(tokens) as nb_hw_matches", "hw_ref_count_matches(tokens) as hw_ref_matches")
nlp_metrics_df = nlp_metrics_df.selectExpr('id', 'subreddit', 'subreddit_id', 'creation_date', 'body', 'nltk_scores.neg as nltk_negativity', 'nltk_scores.neu as nltk_neutrality', 'nltk_scores.pos as nltk_positivity', 'blob_scores.polarity as text_blob_polarity', 'blob_scores.subjectivity as text_blob_subjectivity', 'nb_bw_matches', 'nb_hw_matches', 'hw_ref_matches.intensity as hw_ref_intensity', 'hw_ref_matches.count as nb_hw_ref_matches')

nlp_metrics_df.write.mode('overwrite').parquet('subreddit_sports_nlp.parquet')