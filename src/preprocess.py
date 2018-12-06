
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
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer

###
###
###


# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

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