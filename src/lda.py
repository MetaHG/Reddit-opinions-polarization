import datetime
import re as re

from pyspark.ml.feature import CountVectorizer , IDF
from pyspark.ml.clustering import LDA


def text_preprocessing(txt, stop_words, pos_tagging=False):
    '''
    Take care of doing all the text preprocessing for LDA
    Only works on english ASCII content. (works on content with accent or such, but filter them out)

    All import of nltk 
    '''

    #avoid error for wordnet import
    import nltk 
    nltk_data_str = "./nltk_data.zip/nltk_data"
    if not nltk_data_str in nltk.data.path:
        nltk.data.path.append(nltk_data_str)

    def get_wordnet_pos(treebank_tag):
        from nltk.corpus import wordnet
        if treebank_tag.startswith('J'):
            return wordnet.ADJ
        elif treebank_tag.startswith('V'):
            return wordnet.VERB
        elif treebank_tag.startswith('N'):
            return wordnet.NOUN
        elif treebank_tag.startswith('R'):
            return wordnet.ADV
        else:
            #this is the default behaviour for lemmatize. 
            return wordnet.NOUN

    remove_https = lambda s: re.sub(r'^https?:\/\/.*[\r\n]*', '', s)
    
    #keeping only elements relevant to written speech.
    keep_only_letters = lambda s: re.sub('[^a-zA-Z \']+', '', s)
    
    #remove mark of genitif from speech (i.e. "'s" associated to nouns)
    remove_genitive = lambda s: re.sub('(\'s)+', '', s)
    
    clean_pipeline = lambda s: remove_genitive(keep_only_letters(remove_https(s)))
    
    #tokenizing the texts (removing line break, space and capitalization)
    token_comm = re.split(" ", clean_pipeline(txt).strip().lower())
    
    #to avoid empty token (caused by multiple spaces in the tokenization)
    token_comm = [t for t in token_comm if len(t) > 0]
    
    if pos_tagging:
        from nltk import pos_tag
        token_comm = pos_tag(token_comm)
    else:
        token_comm = zip(token_comm, [None]*len(token_comm))
        
    #removing all words of three letters or less
    bigger_w = [x for x in token_comm if len(x[0]) > 3]

    #removing stop_words
    wout_sw_w = [x for x in bigger_w if x[0] not in stop_words]
    
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import wordnet
    if pos_tagging:
    #get lemma of each word, then return result
        return [WordNetLemmatizer().lemmatize(word, get_wordnet_pos(tag)) for word, tag in wout_sw_w]
    else:
        #get lemma of each word, then return result
        return [WordNetLemmatizer().lemmatize(word) for word, _ in wout_sw_w]

'''
def condense_comm_and_preprocessing(dataset, stop_words):

    #keeping only what matters
    dataset = dataset.select('link_id', 'body', 'created')
    
    zeroValue = ([], datetime.date(year=3016, month=12, day=30))
    combFun = lambda l, r: (l[0] + r[0], min(l[1], r[1]))
    seqFun = lambda prev, curr: (prev[0] + curr[0], prev[1] if prev[1] < curr[1] else curr[1])
    
    #removing post that have been deleted/removed (thus hold no more information)
    filtered = dataset.filter(dataset.body != '[removed]').filter(dataset.body != '[deleted]').filter(dataset.body != '')
    
    #applying preprocessing at the text level, and filtering post with empty tokenization
    filtered_rdd = filtered.rdd.map(lambda r: (r[0], (text_preprocessing(r[1],stop_words, True), r[2]))).filter(lambda r: r[1][0])
    
    #the consequence of the aggregateByKey and zipWithUniqueId makes it that we have tuples of tuples we need to flatten.
    post_and_list_token = filtered_rdd.aggregateByKey(zeroValue, seqFun, combFun).zipWithUniqueId().map(lambda r: (r[0][0], r[0][1][0], r[0][1][1], r[1]))
    
    post_and_list_token = filtered_rdd.aggregateByKey(zeroValue, seqFun, combFun).zipWithUniqueId().map(lambda r: (r[0][0], r[0][1][0], r[0][1][1], r[1]))
    

    return post_and_list_token.toDF().selectExpr("_1 as post_id", "_2 as text","_3 as created", "_4 as uid")
'''

def condense_comm_and_preprocessing(dataset, stop_words, use_pos_tagging=False):
    '''
    Function whose purpose is to condensate all comments
    of one post (identified by link_id) into one array per post 
    in the resulting dataframe. Transform the data into a format
    that is useful to perform LDA on. (considering documents being comment content from post)
    Also apply preprocessing on the textual data. 
    '''
    #keeping only what matters
    dataset = dataset.select('link_id', 'body', 'created')
    
    zeroValue = ([], datetime.date(year=3016, month=12, day=30))
    combFun = lambda l, r: (l[0] + r[0], min(l[1], r[1]))
    seqFun = lambda prev, curr: (prev[0] + curr[0], prev[1] if prev[1] < curr[1] else curr[1])
    
    #removing post that have been deleted/removed (thus hold no more information)
    filtered = dataset.filter(dataset.body != '[removed]').filter(dataset.body != '[deleted]').filter(dataset.body != '')
    
    #applying preprocessing at the text level, and filtering post with empty tokenization
    filtered_rdd = filtered.rdd.map(lambda r: (r[0], (text_preprocessing(r[1],stop_words, use_pos_tagging), r[2]))).filter(lambda r: r[1][0])
    
    #the consequence of the aggregateByKey and zipWithUniqueId makes it that we have tuples of tuples we need to flatten.
    agg_res = filtered_rdd.aggregateByKey(zeroValue, seqFun, combFun)

    post_and_list_token = agg_res.map(lambda r: (r[0], r[1][0], r[1][1]))
    
    withUid = post_and_list_token.zipWithUniqueId().map(lambda r: (r[0][0], r[0][1], r[0][2], r[1]))
    
    return withUid.toDF().selectExpr("_1 as post_id", "_2 as text","_3 as created", "_4 as uid")

def perform_lda(documents, n_topics, n_words, alphas, beta, tokens_col):
    '''
    will perform LDA on a list of documents (== list of token)
    assume that documents is a DataFrame with a column of unique id (uid).
    
    '''
    cv = CountVectorizer(inputCol=tokens_col, outputCol="raw_features")
    cvmodel = cv.fit(documents)
    result_cv = cvmodel.transform(documents)
    
    #we perform an tf-idf (term frequency inverse document frequency), to avoid threads with a lot of words to pollute the topics.
    idf = IDF(inputCol="raw_features", outputCol="features")
    idfModel = idf.fit(result_cv)
    result_tfidf = idfModel.transform(result_cv) 
    
    #keeping created for time series purpose. 
    corpus = result_tfidf.select("uid", "features", "created")
    
    #defining and running the lda. Em is the best optimizer performance-wise.
    #lda = LDA(k=n_topics, optimizer='em', docConcentration=alphas, topicConcentration=beta)
    lda = LDA(k=n_topics, docConcentration=alphas, topicConcentration=beta)
    
    model = lda.fit(corpus)
    
    #retrieving topics, and the vocabulary constructed by the CountVectorizer
    topics = model.describeTopics(maxTermsPerTopic=n_words)
    vocab = cvmodel.vocabulary
    
    #getting topic distribution per document. 
    topic_distribution = model.transform(corpus)[['topicDistribution', 'created']]
    
    #the topics are just numerical indices, we need to convert them to words, and associate them to their weights..
    topics_with_weights = topics.rdd.map(lambda r: (r[0], ([(vocab[t],w) for t,w in zip(r[1], r[2])]), ' '.join([vocab[t] for t in r[1]]))).toDF().selectExpr("_1 as topic_number", "_2 as topic_weight", "_3 as topic")
    
    return topics_with_weights, topic_distribution