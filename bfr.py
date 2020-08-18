from pyspark import SparkContext
from pyspark import SparkConf
import pyspark
import json
import sys, os
from collections import Counter, deque
import time, math, random, csv
from copy import deepcopy as dcopy
import math

conf = SparkConf().setAppName("PySpark App").setMaster("local[*]").set('spark.driver.memory','4g')

sc = SparkContext('local[*]','assignment5',conf=conf)

def run_all(input_file, K, output_file, intermediate_file):
    # THRESHOLD = 0.8
    #K = 10
    ALPHA = 3
    KMEANS_ITERATIONS = 100
    KMEANS_THRESHOLD = 0.5
    
    global cluster_counter
    cluster_counter = 0

    CS_K = 3

    CP_map = dict()

    def calc_average(x):
        return [float(sum(i))/len(i) for i in zip(*x)]

    def euclid_distance(x, y):
        return math.sqrt(sum([(a - b) ** 2 for a, b in zip(x, y)]))

    def get_dataset(filename):
        dataset = list()
        with open(filename, "r") as ptr:
            temp = ptr.readlines()
            for record in temp:
                key, val = record.split(",", 1)
                data = [float(v) for v in val.split(",")]
                dataset.append({"id":key,"values": data})
        return dataset

    def kmeans_converges(old, new):
        highest_change = float('-inf')
            
        for item in old:
            if item == -1:
                continue
            if item in old and item in new:
                old_centroid = old[item]
                new_centroid = new[item]
                change = euclid_distance(new_centroid ,old_centroid)
                if change > highest_change:
                    highest_change = change

        if highest_change < KMEANS_THRESHOLD:
            return True
        return False

    def combine_dicts(old, new):
        for item in new:
            if item in old:
                old[item] += new[item]
            else:
                old[item] = new[item]
        return old

    def get_cluster(point, clusters):
        dist = float('inf')
        currentCluster = None
        for cluster in clusters: 
            if cluster == -1:
                continue
            c_dist = euclid_distance(point["values"],clusters[cluster])
            if c_dist < dist:
                dist = c_dist
                currentCluster = cluster
        return (currentCluster, point)

    def get_sum(data):
            return [sum(i) for i in zip(*data)]
        
    def get_sumsq(data):
        return [sum([i**2 for i in l]) for l in zip(*data)]
        
    def get_DS_params(key, points):
        
        if key == -1:
            return (key, list(points))
        
        point_vals = [ p["values"] for p in points ]
        N = len(points)
        summ = get_sum(point_vals)
        sumq = get_sumsq(point_vals)
        
        return (key, {"N": N,"sum": summ, "sumq": sumq})


    def kmeans_calc(dataset, iterations, k_value, is_DS):
        global cluster_counter
        clusters = dict()
        
        k_value = min(len(dataset), k_value)
        
        for c in random.sample(dataset,k_value):
            clusters[cluster_counter] = c["values"]
            cluster_counter +=1
            
        point_map = dict()
        for itera in range(iterations):
            cluster_point_rdd = sc.parallelize(dataset).map(lambda x: get_cluster(x, clusters)).groupByKey()
            e_step = cluster_point_rdd.mapValues(lambda x: list(x))
            m_step = e_step.mapValues(lambda x: calc_average([temp["values"] for temp in x ])).collectAsMap()
            # print(clusters.keys(), m_step.keys())
            if kmeans_converges(clusters, m_step):
                clusters = dcopy(m_step)
                break
            clusters = dcopy(m_step)
            
        #things to get n, sum, sumq
        if is_DS:
            cluster_point_map = cluster_point_rdd.mapValues(lambda x: [po["id"] for po in x] ).collectAsMap()
            DS = cluster_point_rdd.map(lambda x: get_DS_params(x[0], x[1])).collectAsMap()
            return DS, cluster_point_map
        
        else:
            RS = cluster_point_rdd.filter(lambda x: len(x[1])==1).flatMap(lambda x: list(x[1])).collect()
            CS_rdd = cluster_point_rdd.filter(lambda x: len(x[1])>1)
            cluster_point_map = CS_rdd.mapValues(lambda x: [po["id"] for po in x] ).collectAsMap()
            CS = CS_rdd.map(lambda x: get_DS_params(x[0], x[1])).collectAsMap()
            return CS, RS, cluster_point_map


    def kmeans_plus_plus(dataset, iterations, k_value, is_DS):
        global cluster_counter
        clusters = dict()
        
        k_value = min(len(dataset), k_value)
            
        choice_cluster = random.choice(dataset)
        clusters[cluster_counter] = choice_cluster["values"]
        cluster_counter +=1
        
        for _ in range(k_value-1):
            
            candidate_points = list()
            candidate_dist = list()
            
            for data in dataset:
            
                curr_dist = float('inf')
                
                for cluster in clusters.values():
                    pt_cluster_dist = euclid_distance(data["values"], cluster)
                    if pt_cluster_dist < curr_dist:
                        curr_dist = pt_cluster_dist

                candidate_points.append(data)
                candidate_dist.append(curr_dist)
            
            
            c_index = candidate_dist.index(max(candidate_dist))
            c = candidate_points[c_index]
            
            clusters[cluster_counter] = c["values"]
            cluster_counter +=1
            
    #     for c in random.sample(dataset,k_value):
    #         clusters[cluster_counter] = c["values"]
    #         cluster_counter +=1
            
        point_map = dict()
        for itera in range(iterations):
            print(itera)
            cluster_point_rdd = sc.parallelize(dataset).map(lambda x: get_cluster(x, clusters)).groupByKey()
            e_step = cluster_point_rdd.mapValues(lambda x: list(x))
            m_step = e_step.mapValues(lambda x: calc_average([temp["values"] for temp in x ])).collectAsMap()
            if kmeans_converges(clusters, m_step):
                clusters = dcopy(m_step)
                break
            clusters = dcopy(m_step)
            
        #things to get n, sum, sumq
        if is_DS:
            cluster_point_map = cluster_point_rdd.mapValues(lambda x: [po["id"] for po in x] ).collectAsMap()
            DS = cluster_point_rdd.map(lambda x: get_DS_params(x[0], x[1])).collectAsMap()
            return DS, cluster_point_map
        
        else:
            RS = cluster_point_rdd.filter(lambda x: len(x[1])==1).flatMap(lambda x: list(x[1])).collect()
            CS_rdd = cluster_point_rdd.filter(lambda x: len(x[1])>1)
            cluster_point_map = CS_rdd.mapValues(lambda x: [po["id"] for po in x] ).collectAsMap()
            CS = CS_rdd.map(lambda x: get_DS_params(x[0], x[1])).collectAsMap()
            return CS, RS, cluster_point_map
        
    def mahalanobis_dist(cluster, point):
        
        N = cluster["N"]
        centroids = [ sm/N for sm in cluster["sum"]]
        sd = [ math.sqrt(val/N - centroids[i]**2) for i, val in enumerate(cluster["sumq"]) ]
        temp_sd = list()
        for i in sd:
            if i == 0:
                temp_sd.append(1)
            else:
                temp_sd.append(i)
        dist = math.sqrt(sum( [ ((p-centroids[i])/temp_sd[i])**2 for i, p in enumerate(point) ] ))
        
        return dist

    def is_in_set(cluster_set, point):
        globe_dist = float('inf')
        point_cluster = None
        
        for key in cluster_set:
            dist = mahalanobis_dist(cluster_set[key], point["values"])
            if dist < globe_dist:
                globe_dist = dist
                point_cluster = key
        if globe_dist < ALPHA_THRESHOLD:
            return (True, (point_cluster, point))
        return (False, point)


    def update_DC_Set(Set, new_candidates):
        cp_map = dict()
        for cluster in new_candidates:
            value_list = [x["values"] for x in new_candidates[cluster] ]
            Set[cluster]['N'] += len(value_list)
            summ = get_sum(value_list)
            sumsq = get_sumsq(value_list)
            Set[cluster]['sum'] = get_sum([summ, Set[cluster]['sum']] )
            Set[cluster]['sumq'] = get_sum([sumsq, Set[cluster]['sumq']] )
            cp_map[cluster] = [x["id"] for x in new_candidates[cluster] ]
        return cp_map

    def combo_exists(candidate_key, clusters):
        candidate = clusters[candidate_key]
        
        glob_dist = float("inf")
        combo_cluster = None
        N = candidate["N"]
        centroid = [ sum_val/N for sum_val in candidate["sum"] ]
        
        for cid in clusters:
            if cid == candidate_key or clusters[cid]["N"] < clusters[candidate_key]["N"]:
                continue
            dist = mahalanobis_dist(clusters[cid], centroid)
            if dist < glob_dist:
                glob_dist = dist
                combo_cluster = cid
        if glob_dist < ALPHA_THRESHOLD:
            return True, combo_cluster
        else:
            return False, None


    def combine_clusters(big, small):
        CS[big]["N"] += CS[small]["N"]
        CS[big]["sum"] = get_sum([CS[big]["sum"], CS[small]["sum"]])
        CS[big]["sumq"] = get_sum([CS[big]["sumq"], CS[small]["sumq"]])
        
        CP_map[big] += CP_map[small]
        
        del(CS[small])
        del(CP_map[small])

    intermediate_list = []

    for file_id,f in enumerate(sorted(os.listdir(input_file))):
        print(f)
        if file_id == 0:
            dataset = get_dataset(input_file + f)
        
            temp_CS, RS, temp_cluster_point_map = kmeans_calc(dataset,  KMEANS_ITERATIONS, 5*K, False)
            RS_ids = [ rs["id"] for rs in RS ]
            
            data_len = len(dataset) - len(RS_ids)
            random_indices = set(random.sample(range(0, data_len), int(0.8 * data_len)))

            remaining_dataset = list()
            sample_subset = list()
            
            for i,data in enumerate(dataset):
                if data["id"] in RS_ids:
                    continue
                elif i in random_indices:
                    sample_subset.append(data)
                else:
                    remaining_dataset.append(data)
                    
            D = len(sample_subset[0]['values'])
            ALPHA_THRESHOLD = ALPHA * math.sqrt(D)

            DS, cluster_point_map = kmeans_plus_plus(sample_subset, KMEANS_ITERATIONS, K, True)

            CP_map = combine_dicts(CP_map, cluster_point_map)

            # remaining_dataset = sc.parallelize(dataset).filter(lambda x: x not in sample_subset).collect()

            CS, temp_RS, cluster_point_map = kmeans_calc(remaining_dataset,  KMEANS_ITERATIONS, CS_K*K, False)
            CP_map = combine_dicts(CP_map, cluster_point_map)
            RS += temp_RS
            
        else:

            remaining_dataset = get_dataset(input_file + f)

            for sets in [DS, CS]:
                set_rdd = sc.parallelize(remaining_dataset).map(lambda x: is_in_set(sets,x)).groupByKey()

                set_half = set_rdd.filter(lambda x: x[0]).flatMap(lambda x: tuple(x[1])).groupByKey().mapValues(lambda x: list(x)).collectAsMap()
                remaining_dataset = set_rdd.filter(lambda x: not x[0]).flatMap(lambda x: list(x[1])).collect()

                temp_cp_map = update_DC_Set(sets, set_half)
                CP_map = combine_dicts(CP_map, temp_cp_map)

            for d in remaining_dataset:
                RS.append(d)

            new_CS, RS, cluster_point_map = kmeans_calc(RS,  KMEANS_ITERATIONS, CS_K*K, False)

            CP_map = combine_dicts(CP_map, cluster_point_map)
            CS.update(new_CS)

            flag = True
            while(flag):
                flag = False
                for cluster in CS:
                    exist, cid = combo_exists(cluster, CS)
                    if exist:
                        flag = True
                        combine_clusters(cid, cluster)
                        break
        #printables
        DS_len = len(DS)
        CS_len = len(CS)
        DS_points = CS_points = 0
        for cluster in CP_map:
            if cluster in DS:
                DS_points += len(CP_map[cluster])
            else:
                CS_points += len(CP_map[cluster])
        RS_points = len(RS)
        
        
        print(DS_len, DS_points, CS_len, CS_points, RS_points)
        intermediate_list.append([file_id + 1, DS_len, DS_points, CS_len, CS_points, RS_points])


    def combine_CS_DS(CS_item, DS_item):
        DS[DS_item]["N"] += CS[CS_item]["N"]
        DS[DS_item]["sum"] = get_sum([DS[DS_item]["sum"], CS[CS_item]["sum"]])
        DS[DS_item]["sumq"] = get_sum([DS[DS_item]["sumq"], CS[CS_item]["sumq"]])
        
        CP_map[DS_item] += CP_map[CS_item]
        
        del(CP_map[CS_item])
        del(CS[CS_item])


    def is_CS_combinable(cs_item):
        candidate = CS[cs_item]
        
        glob_dist = float("inf")
        combo_cluster = None
        N = candidate["N"]
        
        centroid = [ sum_val/N for sum_val in candidate["sum"] ]
        
        for did in DS:
            dist = mahalanobis_dist(DS[did], centroid)
            if dist < glob_dist:
                glob_dist = dist
                combo_cluster = did
        if glob_dist < ALPHA_THRESHOLD:
            return True, combo_cluster
        else:
            return False, None

        
    temp_cs = dcopy(CS)
    for cs_item in temp_cs:
        is_combo, ds_item = is_CS_combinable(cs_item)
        if is_combo:
            combine_CS_DS(cs_item, ds_item)

    d = dict()
    for key, values in CP_map.items():
            for val in CP_map[key]:
                d[val] = key

    if RS:
        for outlier in RS:
            d[outlier['id']] = -1

    with open(output_file, 'w') as f:
        json.dump(d,f)


    with open(intermediate_file, mode='w') as preprocess_file:
        iterator = csv.writer(preprocess_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        iterator.writerow(['round_id','nof_cluster_discard','nof_point_discard','nof_cluster_compression','nof_point_compression','nof_point_retained'])
        for l in intermediate_list:
            iterator.writerow(l)


if __name__ == "__main__":
    print ('hi da')
    t = time.time()
    input_file = sys.argv[1]
    K = int(sys.argv[2])
    output_file = sys.argv[3]
    intermediate_file = sys.argv[4]
    run_all(input_file, K, output_file, intermediate_file)
    print ('Time is:', time.time()-t)