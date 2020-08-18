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

def run_all(train_file, model_file, cf_type):

    N = 5

    conf = SparkConf().setAppName("PySpark App").setMaster("local[*]").set('spark.driver.memory','6g')

    sc = SparkContext('local[*]','assignment3',conf=conf)

    if cf_type == 'item_based':

        input = sc.textFile(train_file).map(lambda x:json.loads(x))
        
        def merge_dict(lis_dict):
            temp = dict()
            for entry, rate in lis_dict:
                if entry in temp:
                    temp[entry].append(rate)
                else:
                    temp[entry] = [rate]
            for entry in temp:
                entry_lis = temp[entry]
                temp[entry] = sum(entry_lis)/ len(entry_lis)
            return temp

        business_candidates = input.map(lambda x:(x['business_id'],(x['user_id'], x['stars']))).groupByKey() \
            .filter(lambda x:len(x[1])>=3) \
            .mapValues(merge_dict)


        model_business_user_rating = business_candidates.collectAsMap()

        def pearson_calculator(iterator):
            for ((a,b),(c,d)) in iterator:
                if a < c:
                    intersection = set(b.keys()).intersection(set(d.keys()))
                    if len(intersection)>=3:

                        b_intersect = [b[item] for item in intersection]
                        d_intersect = [d[item] for item in intersection]

                        avg_a = sum(b_intersect)/len(b_intersect)
                        avg_c = sum(d_intersect)/len(d_intersect)

                        numer = d_one = d_two = 0
                        for uid in intersection:
                            numer += (b[uid] - avg_a)*(d[uid] - avg_c)
                            d_one += (b[uid] - avg_a)**2
                            d_two += (d[uid] - avg_c)**2
                        denom = math.sqrt(d_one*d_two)
                        if denom >0 and numer > 0:
                            result = numer/denom
                            yield (a, c, result)

        task_3_1_model = business_candidates.cartesian(business_candidates).mapPartitions(pearson_calculator).collect()

        with open(model_file, 'w') as f:
            for b1, b2, sim in task_3_1_model:
                if sim != 0:
                    d = {"b1": str(b1), "b2": str(b2), "sim": sim}
                    json.dump(d, f)
                    f.write('\n')
                
    if cf_type == 'user_based':
        input = sc.textFile(train_file).map(lambda x:json.loads(x))
        train = input.map(lambda x:(x['user_id'],x['business_id'])).groupByKey().map(lambda x:(x[0],set(x[1])))
        business_dict = input.map(lambda x:x['business_id']).distinct().zipWithIndex().collectAsMap()
        char_matrix = train.map(lambda x:(x[0],[business_dict[u] for u in x[1]]))
        
        def merge_dict(lis_dict):
            temp = dict()
            for entry, rate in lis_dict:
                if entry in temp:
                    temp[entry].append(rate)
                else:
                    temp[entry] = [rate]
            for entry in temp:
                entry_lis = temp[entry]
                temp[entry] = sum(entry_lis)/ len(entry_lis)
            return temp

        user_business_ratings = input.map(lambda x:(x['user_id'],(x['business_id'], x['stars']))).groupByKey() \
            .mapValues(merge_dict).collectAsMap()

        num_hashes = 50
        a = list(range(1,num_hashes))
        b = list(range(5*num_hashes, 7*num_hashes))
        p = [100003,100019,100049,100057,100129,100151,100153,100237,100699,100801,101009,101111,102001,101869,102043]
        m = len(business_dict)
        num_bands = num_hashes
        num_rows = int(num_hashes/num_bands)
        threshold = 0.01

        len_a = len(a)
        len_b = len(b)
        len_p = len(p)
        coeffs = list((a[i%len_a],b[i%len_b],p[i%len_p]) for i in range(num_hashes))

        def hash_func(x,coeff):
            return (((coeff[0]*x) + coeff[1]) % coeff[2]) % m

        def construct_signature(cols,coeffs):
            return [min([hash_func(c,coeff) for c in cols]) for coeff in coeffs]

        sig_matrix = char_matrix.map(lambda x:(x[0],construct_signature(x[1],coeffs)))

        banding = sig_matrix \
            .flatMap(lambda x: [( (key,",".join([ str(f) for f in x[1][key:key+num_rows]]) ) , x[0]   )  for key in range(0, num_hashes, num_rows)]) \
            .groupByKey().map(lambda x: (x[0], list(x[1]))) \
            .filter(lambda x: len(x[1])>1)

        def jaccard_partition(iterator):
            for u1, u2 in iterator:
                a = set(user_business_ratings[u1].keys())
                b = set(user_business_ratings[u2].keys())
                intersection_len = len(a.intersection(b))
                if intersection_len > 2:
                    jaccard = intersection_len / len(a.union(b))
                    if jaccard >= threshold:
                        yield (u1, u2)

        jaccard_result = banding.flatMap(lambda x: list(combinations(x[1],2))) \
                    .mapPartitions(jaccard_partition)

        def pearson_calculator(iterator):
            for a,c in iterator:
                if a != c:
                    b = user_business_ratings[a]
                    d = user_business_ratings[c]
                    intersection = set(b.keys()).intersection(set(d.keys()))

                    b_intersect = [b[item] for item in intersection]
                    d_intersect = [d[item] for item in intersection]

                    avg_a = sum(b_intersect)/len(b_intersect)
                    avg_c = sum(d_intersect)/len(d_intersect)

                    numer = d_one = d_two = 0
                    for bid in intersection:
                        numer += (b[bid] - avg_a)*(d[bid] - avg_c)
                        d_one += (b[bid] - avg_a)**2
                        d_two += (d[bid] - avg_c)**2
                    denom = math.sqrt(d_one*d_two)
                    if denom > 0 and numer > 0:
                        result = numer/denom
                        yield (a, c, result)

        pearson_result = jaccard_result.mapPartitions(pearson_calculator)

        task_3_2_model = pearson_result.collect()

        with open(model_file, 'w') as f:
            for u1, u2, sim in task_3_2_model:
                d = {"u1": str(u1), "u2": str(u2), "sim": sim}
                json.dump(d, f)
                f.write('\n')

if __name__ == "__main__":
    t = time.time()
    train_file = sys.argv[1]
    model_file = sys.argv[2]
    cf_type = sys.argv[3]
    run_all(train_file, model_file, cf_type)
    print ('time is :', time.time()-t)
