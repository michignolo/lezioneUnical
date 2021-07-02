#!/usr/bin/python
## read network data from redis and create json (for html visualization) and gexf for gephi
import redis
import networkx as nx
import community
import json
import sys
from networkx.readwrite import json_graph

r = redis.StrictRedis(host='localhost', port=6379, db=0)

DG = nx.DiGraph()
for key in r.scan_iter("link*"):
    try:
        start, stop = str(key).replace("b'","").replace("'","").replace("link_","").split("_-_")
        n = str(key).replace("link_","").replace("b'","").replace("'","")
        try:
            w = int(r.hget(key,n))
        except:
            w = 1
        #print(start,stop,w)
        if(start != stop):
            DG.add_edge(start,stop, weight=w)
    except:
        pass




DGG = nx.k_core(DG, int(sys.argv[1])) ### pass the minimum  degree to consider
bp = community.best_partition(DGG.to_undirected())
for n in DGG.nodes():
    DGG.nodes[n]['group'] = bp[n]

data1 = json_graph.node_link_data(DGG)

s1 = json.dumps(data1)
op = open("out.json", "w") ## this the output for the web (d3js)
op.write(s1)
op.close()

nx.write_gexf(DG,"net.gexf") ## this the output for gephi
