
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

# # Load and preprocess sample data
_, messages = load_data(sc, filter=[2016])

# Prepare stopwords, stemmer and lemmatizer for messages preprocessing.
en_stopwords = stopwords.words('english')
en_stemmer = SnowballStemmer('english')
en_lemmatizer = WordNetLemmatizer()

# Clean messages
cleaned_messages = messages.filter("body != '[removed]' and body != '[deleted]'")


def process_body(body, n_grams=1, left_pad_symbol=None, right_pad_symbol=None, lemmatizer=None, stemmer=None, \
                stop_words=None, lemmatize_stop_words=False, stem_stop_words=False, remove_stop_words=False):
    """
    Process the message bodies of the given rdd
        
    Parameters:
        body: 
            string message body
        n_gram: 
            size of the n_grams in the rdd output
        lemmatizer: 
            lemmatizer to use on the message words. If None, words are not lemmatize
        stemmer: 
            stemmer to use on the message words. If None, words are not stemmed.
        stop_words: 
            list of words to consider as stop words
        lemmatize_stop_words: 
            boolean to lemmatize stop words
        stem_stop_words: 
            boolean to stem stop words
        remove_stop_words: 
            boolean to remove stop words from the tokens
        
    Returns:
        rdd of the form (parent_id, id, processed_msg_body)
    """
    
    if n_grams < 1:
        raise ValueError("n_grams should be bigger than 1")
    
    tknzr = TweetTokenizer()
    tokens = tknzr.tokenize(body)
    
    if stop_words is None:
        stop_words = []
    if lemmatizer is not None and stemmer is not None:
        if remove_stop_words:
            tokens = [lemmatizer.lemmatize(stemmer.stem(token)) for token in tokens if token not in stop_words]
        elif not lemmatize_stop_words and not stem_stop_words:
            tokens = [lemmatizer.lemmatize(stemmer.stem(token)) if token not in stop_words else token for token in tokens]
        elif not lemmatize_stop_words:
            tokens = [lemmatizer.lemmatize(stemmer.stem(token)) if token not in stop_words else stemmer.stem(token) for token in tokens]
        elif not stem_stop_words:
            tokens = [lemmatizer.lemmatize(stemmer.stem(token)) if token not in stop_words else lemmatizer.lemmatize(token) for token in tokens]
    elif lemmatizer is not None:
        if remove_stop_words:
            tokens = [lemmatizer.lemmatize(token) for token in tokens if token not in stop_words]
        elif not lemmatize_stop_words:
            tokens = [lemmatizer.lemmatize(token) if token not in stop_words else token for token in tokens]
    elif stemmer is not None:
        if remove_stop_words:
            tokens = [stemmer.stem(token) for token in tokens if token not in stop_words]
        elif not stem_stop_words is not None:
            tokens = [stemmer.stem(token) if token not in stop_words else token for token in tokens]
    elif stemmer is not None and lemmatizer is not None:
            tokens = [token for token in tokens if token not in stop_words]

    if left_pad_symbol is not None and right_pad_symbol is not None:
        tokens = list(nltk.ngrams(tokens, n_grams, True, True, left_pad_symbol, right_pad_symbol))
    elif left_pad_symbol is not None:
        tokens = list(nltk.ngrams(tokens, n_grams, pad_left=True, left_pad_symbol=left_pad_symbol))
    elif right_pad_symbol is not None:
        tokens = list(nltk.ngrams(tokens, n_grams, pad_right=True, right_pad_symbol=right_pad_symbol))
    else:
        tokens = list(nltk.ngrams(tokens, n_grams))

    return [list(token) for token in tokens]

# Adapt function for SQL
process_body_udf = func.udf(process_body, ArrayType(ArrayType(StringType(), False), False))
spark.udf.register('process_body', process_body, ArrayType(ArrayType(StringType(), False), False))


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

###
### Sentence polarity using TextBlob
###

# Using simple sentence polarity analysis
def compute_blob_polarity(msg_body):
    sentiment = TextBlob(msg_body).sentiment
    return {'polarity': sentiment.polarity, 'subjectivity': sentiment.subjectivity}

compute_blob_polarity_udf = func.udf(compute_blob_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_blob_polarity', compute_blob_polarity_udf)


# Using twitter trained positive/negative naive bayes classifier
def compute_blob_class_polarity(msg_body):
    pol_class = TextBlob(msg_body, analyzer=NaiveBayesAnalyzer()).sentiment
    return {'classification': -1 if pol_class.classification == 'neg' else 1, 'p_pos': pol_class.p_pos, 'p_neg': pol_class.p_neg}

compute_blob_class_polarity_udf = func.udf(compute_blob_class_polarity, MapType(StringType(), FloatType(), False))
spark.udf.register('compute_blob_class_polarity', compute_blob_class_polarity_udf)

# This does not finish, classifier takes too long


###
### Other metrics (Vulgarity, hate speech)
###

# Process tokens
tokens = cleaned_messages.selectExpr('id', 'created_utc', 'process_body(body) as tokens')

# Define helper functions
def count_matches(msg_grams, ref_grams, ref_grams_intensity=None):
    """
    Compute for each gram in msg_grams the number of occurences of words contained in ref_grams.
    Also compute the intensity scores for each message if ref_grams_intensity is given.

    Parameters:
        msg_grams: list of grams [['gram0_word0', 'gram0_word1'], ['gram1_word0', 'gram1_word1']]
        ref_grams: list of reference words ['Hello', 'how', 'are', 'you']
        ref_grams_intensity: list of weights for reference words [0.5, 0.7, 0.3, 0.2]

    Returns:
        Number of matches, intensity score (if ref_grams_intensity not None)
    """

    msg_grams_joined = [' '.join(msg_gram) for msg_gram in msg_grams]
    msg_grams_counter = Counter(msg_grams_joined)
    count = 0.0
    intensity = 0.0
    for i, ref_gram in enumerate(ref_grams):
        count = count + msg_grams_counter[ref_gram]
        if ref_grams_intensity is not None:
            intensity = intensity + msg_grams_counter[ref_gram] * ref_grams_intensity[i]
    
    if ref_grams_intensity is None:
        return count
    else: 
        return {'count':count, 'intensity':intensity}
    
def df_count_matches(gram_list, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_list), FloatType())
    spark.udf.register(sql_fun_name, udf)

def df_count_matches_intensity(gram_list, intensity_list, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_list, intensity_list), MapType(StringType(), FloatType()))
    spark.udf.register(sql_fun_name, udf)

### Vulgarity
bad_words = spark.read.csv('lexicons/bad_words_lexicon/en.csv', header=True)
bw_gram_rank = bad_words.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('en_bad_words')))

bw_1_grams = [i.en_bad_words for i in bw_gram_rank.filter('gram_rank == 1').select('en_bad_words').collect()]


# Hate speech
## Raw hate words (basic)
hate_words = spark.read.csv('lexicons/hatespeech_lexicon/hatebase_dict.csv', header=True)
hate_words = hate_words.withColumnRenamed("uncivilised',", 'hate_words')                         .withColumn('hate_words', func.udf(lambda d: d[1:-2])(func.col('hate_words')))
hw_gram_rank = hate_words.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('hate_words')))

hw_1_grams = [i.hate_words for i in hw_gram_rank.filter('gram_rank == 1').select('hate_words').collect()]


## Refined hate words
hw_ref_schema = StructType([StructField('hate_words_ref', StringType(), False), StructField('intensity', FloatType(), False)])
hate_words_ref = spark.read.csv('lexicons/hatespeech_lexicon/refined_ngram_dict.csv', header=True, schema=hw_ref_schema)
hw_ref_gram_rank = hate_words_ref.withColumn('gram_rank', func.udf(lambda gram: len(gram.split()), IntegerType())(func.col('hate_words_ref')))

hw_ref_1_grams = [i.hate_words_ref for i in hw_ref_gram_rank.filter('gram_rank == 1').select('hate_words_ref').collect()]
hw_ref_1_intensity = [i.intensity for i in hw_ref_gram_rank.filter('gram_rank == 1').select('intensity').collect()]


# Compute all metrics
df_count_matches(bw_1_grams, 'bw_count_matches')
df_count_matches(hw_1_grams, 'hw_count_matches')
df_count_matches_intensity(hw_ref_1_grams, hw_ref_1_intensity, 'hw_ref_count_matches')

nlp_metrics_df = cleaned_messages.selectExpr('id', 'created_utc', 'body', 'process_body(body) as tokens')
nlp_metrics_df = nlp_metrics_df.selectExpr('id', 'created_utc', 'body', "compute_nltk_polarity(body) as nltk_scores", "compute_blob_polarity(body) as blob_scores", "bw_count_matches(tokens) as nb_bw_matches", "hw_count_matches(tokens) as nb_hw_matches", "hw_ref_count_matches(tokens) as hw_ref_matches")
nlp_metrics_df = nlp_metrics_df.selectExpr('id', 'created_utc', 'body', 'nltk_scores.neg as nltk_negativity', 'nltk_scores.neu as nltk_neutrality', 'nltk_scores.pos as nltk_positivity', 'blob_scores.polarity as text_blob_polarity', 'blob_scores.subjectivity as text_blob_subjectivity', 'nb_bw_matches', 'nb_hw_matches', 'hw_ref_matches.intensity as hw_ref_intensity', 'hw_ref_matches.count as nb_hw_ref_matches')

nlp_metrics_df = nlp_metrics_df.withColumn('created_utc', func.from_unixtime(nlp_metrics_df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType())) \
                                .withColumnRenamed('created_utc', 'creation_date')

nlp_metrics_df.registerTempTable("nlp_metrics")

# Compute daily average metrics
daily_nlp_metrics = spark.sql("""
SELECT
    creation_date,
    
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
    ) AS nltk_positivity_60d_avg,
    
    AVG(sum_text_blob_polarity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS text_blob_polarity_60d_avg,
    
    AVG(sum_text_blob_subjectivity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS text_blob_subjectivity_60d_avg,
    
    AVG(sum_nb_bw_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_bw_matches_60d_avg,
    
    AVG(sum_nb_hw_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_hw_matches_60d_avg,
    
    AVG(sum_hw_ref_intensity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS hw_ref_intensity_60d_avg,
    
    AVG(sum_nb_hw_ref_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_hw_ref_matches_60d_avg
    
FROM (
    SELECT
        creation_date,
        SUM(nltk_negativity) AS sum_nltk_negativity,
        SUM(nltk_neutrality) AS sum_nltk_neutrality,
        SUM(nltk_positivity) AS sum_nltk_positivity,
        SUM(text_blob_polarity) AS sum_text_blob_polarity,
        SUM(text_blob_subjectivity) AS sum_text_blob_subjectivity, 
        SUM(nb_bw_matches) AS sum_nb_bw_matches,
        SUM(nb_hw_matches) AS sum_nb_hw_matches,
        SUM(hw_ref_intensity) AS sum_hw_ref_intensity,
        SUM(nb_hw_ref_matches) AS sum_nb_hw_ref_matches
    FROM nlp_metrics
    GROUP BY creation_date
    ORDER BY creation_date
)
""")

daily_nlp_metrics.write.mode('overwrite').parquet('daily_nlp_metrics.parquet')
#daily_nlp_metrics.write.csv('daily_nlp_metrics.tsv', mode='overwrite', compression='gzip', sep='\t', header=True, timestampFormat='yyyy-MM-dd HH:mm:ss.SS')
