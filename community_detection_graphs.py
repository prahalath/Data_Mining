from pyspark import SparkContext
from pyspark import SparkConf
import pyspark
import json
import sys
from itertools import combinations, chain
from collections import Counter, deque
import time

def run_all(threshold, input_file, betweeness_file, community_file):
    conf = SparkConf().setAppName("PySpark App").setMaster("local[*]").set('spark.driver.memory','4g')

    sc = SparkContext('local[*]','assignment4',conf=conf)
    sc.setLogLevel("ERROR")


    data = sc.textFile(input_file) \
            .map(lambda line: line.split(",")) \
            .filter(lambda line: len(line)>1) \
            .map(lambda line: (line[0],line[1])) \
            .filter(lambda x: x[0] not in 'user_id')

    set_data = data.groupByKey() \
            .mapValues(lambda x:set(x)) \
            .filter(lambda x: len(x[1])>=threshold)

    def process(iterator):
        for (a,b), (c,d) in iterator:
            if a<c and len(b.intersection(d)) >= threshold:
                yield(a,c)
        
    graph = set_data.cartesian(set_data).mapPartitions(process)

    nodes = graph.flatMap(lambda x: x).distinct()

    edges_dict = graph \
                    .flatMap(lambda x: [[x[0], x[1]], [x[1], x[0]]]) \
                    .groupByKey() \
                    .mapValues(lambda x: set(x)) \
                    .collectAsMap()

    k = dict()
    for key in edges_dict:
        k[key] = len(edges_dict[key])

    def bfs(iterator):
        for node in iterator:
            visited = set()
            child_set = set()
            parent_dict = dict()
            label_dict = dict()
            level_dict = dict()
            root = node
            queue = deque()
            queue.append(node)
            
            max_level = 0
            level_dict[node] = 0
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                
                #Assign label values to the nodes
                if current in parent_dict:
                    label_dict[current] = sum([label_dict[parent_curr] for parent_curr in parent_dict[current]] )
                else:
                    label_dict[current] = 1
                
                parent = current
                #Construct Child and Parent Dicts
                for child in edges_dict[parent]:
                    if child not in level_dict:
                        queue.append(child)
                        level_dict[child] = level_dict[parent] + 1
                        if max_level < level_dict[child]:
                            max_level = level_dict[child]
                        
                    if level_dict[child] == level_dict[parent] + 1:
                        if parent not in child_set:
                            child_set.add(parent)

                        if child not in parent_dict:
                            parent_dict[child] = set([parent])
                        else:
                            parent_dict[child].add(parent)

            leaves = visited.difference( child_set )

            credit_dict = btw_dict = dict()
            
            for node in visited:
                credit_dict[node] = 1

            level_list = [[] for i in range(max_level+1)]
            for key, val in level_dict.items():
                level_list[val].append(key)
            level_list.reverse()
            for node_list in level_list:
                for node in node_list:
                    if node in parent_dict:
                        parent_list = parent_dict[node]
                        sum_labels = sum( [label_dict[parent] for parent in parent_list] )
                        for parent in parent_list:
                            parent_fraction = label_dict[parent]/sum_labels
                            total_fraction = credit_dict[node] * parent_fraction
                            credit_dict[parent] += total_fraction
                            if node < parent:
                                yield ( (node,parent), total_fraction )
                            else:
                                yield ( (parent,node), total_fraction )

    edge_betweeness = nodes.mapPartitions(bfs) \
                        .groupByKey() \
                        .mapValues(lambda x: sum(x)/2) \
                        .collect()

    edge_betweeness = sorted(edge_betweeness, key=lambda x:x[1], reverse=True)


    with open(betweeness_file,'w') as file:
        for i in edge_betweeness:
            t = str(i[0])+str(', ')+str(i[1])
            file.write(t)
            file.write('\n')

    # Modularity

    def get_communities(edge_list):
        community_list = list()
        node_pointers = dict()
        for (a,b), val in edge_list:
            if a in node_pointers:
                node_pointers[a].add(b)
            else:
                node_pointers[a] = set([b])
            if b in node_pointers:
                node_pointers[b].add(a)
            else:
                node_pointers[b] = set([a])
                
        nodes = set(node_pointers.keys())
        super_visited = set()
        
        for node in nodes:
            if node in super_visited:
                continue
            visited = set()
            queue = deque()
            queue.append(node)
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                for child in node_pointers[current]:
                    queue.append(child)
                    
            super_visited = super_visited.union(visited)
            community_list.append(visited)
        return community_list

    A = set(graph.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).collect())

    m = graph.count()

    global_Q = 0
    global_community = list()

    def calc_Q(communities):
        current_Q = 0
        for community in communities:
            for i in community:
                ki = k[i]
                for j in community:
                    if i != j:
                        kj = k[j]
                        a = 0
                        if (i,j) in A:
                            a = 1
                        current_Q += (a - ki*kj/m2)
        current_Q /= m2
        return current_Q
    
    current_betweenness = edge_betweeness[:]
    m2 = 2*m
    communities = get_communities(current_betweenness)
    current_Q = calc_Q(communities)
    if current_Q > global_Q:
        global_Q = current_Q
        global_community = current_betweenness[:]
        
    for x in range(len(edge_betweeness)-1):
        
        ((a,b), edg_value) = current_betweenness.pop(0)
        if a in edges_dict:
            if b in edges_dict[a]:
                edges_dict[a].remove(b)
        if b in edges_dict:
            if a in edges_dict[b]:
                edges_dict[b].remove(a)
                
        current_betweenness = nodes.mapPartitions(bfs).groupByKey().mapValues(lambda x: sum(x)/2).collect()
        current_betweenness = sorted(current_betweenness, key=lambda x:x[1], reverse=True)
        communities = get_communities(current_betweenness)
        current_Q = calc_Q(communities)
        if current_Q > global_Q:
            global_Q = current_Q
            global_community = communities

    
    community_rdd = []
    for i,v in enumerate(global_community):
        community_rdd.append((i,v))

    final_comm = sc.parallelize(community_rdd).mapValues(lambda x:sorted(list(x))) \
                    .sortBy(lambda x:(len(x[1]),x[1])) \
                    .map(lambda x:x[1]) \
                    .collect()

        
    with open(community_file,'w') as file:
        for comm in final_comm:
            line = ', '.join('\''+str(c)+'\'' for c in comm)
            file.write(line)
            file.write('\n')


if __name__ == "__main__":
    t = time.time()
    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweeness_file = sys.argv[3]
    community_file = sys.argv[4]
    run_all(threshold, input_file, betweeness_file, community_file)
    print ('time is:', time.time()-t)
