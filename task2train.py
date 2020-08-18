from pyspark import SparkContext
from pyspark import SparkConf
import pyspark
import json
import sys
from itertools import combinations, chain
from collections import Counter
import pickle
import re
import math
import time

def run_all(train_file, model_file, stopwords):

    conf = SparkConf().setAppName("PySpark App").setMaster("local[*]").set('spark.driver.memory','4g')

    sc = SparkContext('local[*]','assignment3',conf=conf)

    input = sc.textFile(train_file).map(lambda x:json.loads(x))

    stop = stopwords
    stopwords_list = []
    f = open(stop)
    for x in f.readlines():
        stopwords_list.append(x.strip())
    stopwords_list.append("")

    business_text = input.map(lambda x:(x['business_id'],x['text'])).groupByKey().map(lambda x:(x[0]," ".join(x[1])))

    def preprocess(x):
        splitted = x.split(" ")
        result = list()
        for i in splitted:
            temp = re.sub('[^a-z]+', '', i.lower())
            temp = temp.strip()
            if temp not in stopwords:
                result.append(temp)
        return " ".join(result)

    true_text = business_text.mapValues(preprocess)

    ct = true_text.flatMapValues(lambda x:x.split()) \
            .count()

    freq_thre = int((0.000001)*ct)

    non_words = true_text.flatMap(lambda x:x[1].split()) \
            .map(lambda x:(x,1)) \
            .reduceByKey(lambda a,b:a+b) \
            .filter(lambda x:x[1]<freq_thre) \
            .collectAsMap()

    def remove_less_freq(text):
        result = []
        text = text.split()
        for word in text:
            if word not in non_words:
                result.append(word)
        return ' '.join(result)

    true_text = true_text.mapValues(remove_less_freq)

    N = true_text.count()

    idf = true_text.flatMap(lambda x: [(i, x[0]) for i in x[1].split()]).groupByKey().mapValues(lambda x: math.log(N/len(set(x)),2)).collectAsMap()

    def tf_idf_calc(text):
        tf_idf = []
        count = Counter(text.split())
        max_count = count.most_common(1)[0][1]
        for k,v in count.items():
            tf_idf.append((k,idf[k]*(v/max_count)))
        tf_idf.sort(key = lambda x: x[1], reverse=True)
        sortid = tf_idf[:200]
        return set([a for (a,b) in sortid])

    business_profile = true_text.mapValues(tf_idf_calc)

    tf_idf = business_profile.collectAsMap()

    user_bus_list = input.map(lambda x:(x['user_id'],x['business_id'])).groupByKey().collectAsMap()


    model = (tf_idf, user_bus_list)

    with open('task2.model', 'wb') as file:
        pickle.dump(model, file, protocol=pickle.HIGHEST_PROTOCOL)



if __name__ == "__main__":
    t = time.time()
    train_file = sys.argv[1]
    model_file = sys.argv[2]
    stopwords = sys.argv[3]
    run_all(train_file, model_file, stopwords)
    print ('time is:', time.time()-t)
