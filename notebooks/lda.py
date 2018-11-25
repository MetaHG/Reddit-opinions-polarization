import findspark
findspark.init()

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
comments = spark.read.load('../data/sample.parquet')
comments = comments.withColumn('created', func.from_unixtime(comments['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
comments.registerTempTable("comments")
sc = spark.sparkContext

import spacy
import gensim
from spacy.lang.en import English
import nltk
import re
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet as wn
import datetime
en_stop = set(nltk.corpus.stopwords.words('english'))
parser = English()


def lda_on_comments(dataset, words_per_topic, n_topics, parser=English(), stop_words=en_stop):
    '''   
    This function performs a LDA (Latent Dirichlet Allocation) model on a set of reddit comments.
    Useful for topic modelling/extraction from a reddit post.
    Parameters
    −−−−−−−−−−
    dataset: pyspark RDD or Dataframe, schema should have only three data type : 
              the post id (link_id), the body of the comment and the creation date in this order.
              
    words_per_topic: number of words that should constitute a topic per post.
    
    n_topics: number of topics to extract by post
    
    parser: the natural language parser used, corresponds to a language normally,
            by default english (as it is the most used language on reddit).
            should be a parser from the spacy.lang library.
    
    stop_words: set of words that constitutes stop words (i.e. that should be
                removed from the tokens)

    Returns
    −−−−−−−
    A RDD with the following pair of data as rows : (<post_id>, <topic (as a list of words)>)) 
    '''
    #useful functions for preprocessing the data for LDA
    def tokenize(text):
        lda_tokens = []
        tokens = parser(text)
        for token in tokens:
            if token.orth_.isspace():
                continue
            elif token.like_url:
                lda_tokens.append('URL')
            elif token.orth_.startswith('@'):
                lda_tokens.append('SCREEN_NAME')
            else:
                lda_tokens.append(token.lower_)
        return lda_tokens

    def get_lemma(word):
        lemma = wn.morphy(word)
        if lemma is None:
            return word
        else:
            return lemma

    def get_lemma2(word):
        return WordNetLemmatizer().lemmatize(word)

    def prepare_text_for_lda(text):
        tokens = tokenize(text)
        tokens = [token for token in tokens if len(token) > 4]
        tokens = [token for token in tokens if token not in en_stop]
        tokens = [get_lemma(token) for token in tokens]
        return tokens
    
    def get_n_topics(text_data):
        dictionary = gensim.corpora.Dictionary(text_data)
        corpus = [dictionary.doc2bow(text) for text in text_data]
        ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = n_topics, id2word=dictionary, passes=15)
        topics = ldamodel.print_topics(num_words=words_per_topic)
        return topics
    
    def extract_key_words(lda_result):
        return re.findall(r'\"(.*?)\"', lda_result)

    #detecting type of the input given.
    if isinstance(dataset, pyspark.sql.dataframe.DataFrame):
        dataset = dataset.rdd
    elif not isinstance(dataset, pyspark.rdd.RDD):
        raise ValueError('Wrong type of dataset, must be either a pyspark RDD or pyspark DataFrame')
    
    #TODO : keep the minimum timestamp (r[2] of the dataset) during computations.
    
    #filtering comments that were removed, to avoid them to pollute the topics extracted
    filter_absent_comm = dataset.filter(lambda r: r[1] != '[removed]' and r[1] != '[deleted]')
    
    #applying text preprocesisng for LDA + filtering all empty sets (without tokens as the result of the LDA preprocessing)
    LDA_preprocessed = filter_absent_comm.map(lambda r: (r[0], list(prepare_text_for_lda(r[1])))).filter(lambda r: r[1])
    
    #groupy every comments by post/thread id.
    post_and_list_token = LDA_preprocessed.groupByKey().map(lambda x : (x[0], list(x[1])))
    
    #generating n topics per post/thread.
    res_lda = post_and_list_token.map(lambda r: (r[0],get_n_topics(r[1]))).flatMap(lambda r: [(r[0], t) for t in r[1]])
    
    return res_lda.map(lambda r: (r[0], ' '.join(extract_key_words(r[1][1])))).toDF().selectExpr("_1 as post_id", "_2 as topic")


def lda_on_posts(dataset, n_topics, n_words):
    #The post entry in the df/rdd is an id, followed by a disordered list of 
    def tokenize(text):
        lda_tokens = []
        tokens = parser(text)
        for token in tokens:
            if token.orth_.isspace():
                continue
            elif token.like_url:
                lda_tokens.append('URL')
            elif token.orth_.startswith('@'):
                lda_tokens.append('SCREEN_NAME')
            else:
                lda_tokens.append(token.lower_)
        return lda_tokens

    def get_lemma(word):
        lemma = wn.morphy(word)
        if lemma is None:
            return word
        else:
            return lemma

    def get_lemma2(word):
        return WordNetLemmatizer().lemmatize(word)

    def prepare_text_for_lda(text):
        tokens = tokenize(text)
        tokens = [token for token in tokens if len(token) > 4]
        tokens = [token for token in tokens if token not in en_stop]
        tokens = [get_lemma(token) for token in tokens]
        return tokens

    def get_n_topics(text_data):
        dictionary = gensim.corpora.Dictionary(text_data)
        
        corpus = [dictionary.doc2bow(text) for text in text_data]
        ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = n_topics, id2word=dictionary, passes=15)
        topics = ldamodel.print_topics(num_words=n_words)
        return topics

    def extract_key_words(lda_result):
        return re.findall(r'\"(.*?)\"', lda_result)
    
    #applying text preprocesisng for LDA + filtering all empty sets (without tokens as the result of the LDA preprocessing)
    preprocessed = dataset.map(lambda r: (r[0], [prepare_text_for_lda(comm) for comm in r[1]], r[2]))
    
    #this may seem a bit unnecesseray, but it prevents rare posts whose text has been cleansed by stop-words.
    preprocessed_filtered = preprocessed.filter(lambda r: r[1]).filter(lambda r: r[1][0])
    
    #generating n topics per post/thread.
    res = preprocessed_filtered.map(lambda r: (r[0],get_n_topics(r[1]), r[2])).flatMap(lambda r: [(r[0], t, r[2]) for t in r[1]])
    
    return res.map(lambda r: (r[0], ' '.join(extract_key_words(r[1][1])), r[2])).toDF().selectExpr("_1 as post_id", "_2 as topic", "_3 as created")



