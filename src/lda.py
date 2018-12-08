import pyspark
from pyspark.sql.types import *
from pyspark import SQLContext

from nltk.corpus import wordnet as wn

import datetime
import re as re

from pyspark.ml.feature import CountVectorizer , IDF
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.mllib.clustering import LDA

def reddit_comment_preprocessing(txt, stop_words):
    '''
    Take care of doing all the text preprocessing for LDA at the comment level
    Only works on english ASCII content. (works on content with accent or such, but filter them out)
    '''
    #TODO : maybe remove http links.
    
    #keeping only elements relevant to written speech.
    keep_only_letters = lambda s: re.sub('[^a-zA-Z \']+', '', s)
    
    #remove mark of genitif from speech (i.e. "'s" associated to nouns)
    remove_genitive = lambda s: re.sub('(\'s)+', '', s)
    
    #tokenizing the texts (removing line break, space and capitalization)
    token_comm = re.split(" ", remove_genitive(keep_only_letters(txt)).strip().lower())
    
    #removing all words of two letters or less
    bigger_w = [x for x in token_comm if len(x) > 2] 
    
    #removing stop_words
    wout_sw_w = [x for x in bigger_w if x not in stop_words]
    
    def get_lemma(word):
        lemma = wn.morphy(word)
        return word if lemma is None else lemma
    
    #get lemma of each word, then return result
    return [get_lemma(token) for token in wout_sw_w]


def dataset_cleaning_and_preprocessing(data, stop_words):
    '''
    take a pyspark dataframe as input,
    transform it into a RDD, and filter all 
    empty comments ([removed], [deleted], or just empty text)
    and apply the preprocessing for lda.
    '''
    #keeping only what matters
    dataset = data.select('link_id', 'body', 'created')
    
    #removing post that have been deleted/removed (thus hold no more information)
    filtered = dataset.filter(dataset.body != '[removed]').filter(dataset.body != '[deleted]').filter(dataset.body != '')
    
    #applying comment preprocessing for LDA
    preprocessed = filtered.rdd.map(lambda r: (r[0], reddit_comment_preprocessing(r[1], stop_words), r[2]))
    
    #we return only posts which have actual textual comments. (hence the filter with the second row element (pythonic way of testing non emptiness of list))
    preprocessed_filtered = preprocessed.filter(lambda r : r[1])
    
    return preprocessed.map(lambda r: (r[0], (r[1], r[2])))


def perform_lda(documents, n_topics, n_words):
    '''
    will perform LDA on a list of documents (== list of token)
    assume that documents is a RDD.
    '''
    documents_df = documents.zipWithUniqueId().toDF().selectExpr("_1 as tokens", "_2 as uid")
    
    #TODO : fine tune parameters
    cv = CountVectorizer(inputCol="tokens", outputCol="raw_features")
    cvmodel = cv.fit(documents_df)
    result_cv = cvmodel.transform(documents_df)
    
    #we perform an tf-idf (term frequency inverse document frequency), to avoid comments with a lot of words to pollute the topics.
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv) 
    
    corpus = result_tfidf.select("uid", "features").rdd.map(lambda r: [r[0],Vectors.fromML(r[1])])
    model = LDA.train(corpus, k=n_topics)

    topics = model.describeTopics(maxTermsPerTopic=n_words)
    
    vocab = cvmodel.vocabulary
    
    terms = topics[0]
    
    #topic[0] represents the words, topic[1] their weights
    return [[(vocab[topic[0][n]], topic[1][n]) for n in range(len(topic[0]))] for _, topic in enumerate(topics)]



def lda_and_min_date(in_rdd, n_topics, n_words):
    tok_date = in_rdd.values()
    min_date = tok_date.values().min()
    topics = perform_lda(tok_date.keys(), n_topics, n_words)
    return (topics, min_date)