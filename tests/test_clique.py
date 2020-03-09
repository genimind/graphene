import os
import unittest
import json
import networkx as nx
import igraph as ig
from graphene import graphgen

node_mapper_filename   = './node_mapper.json'
clique_mapper_filename = './clique_mapper.json'
data_filename          = './test_data.txt'

class TestClique(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.clique_mapper = None
        self.data = []
        with open(node_mapper_filename) as f:
            self.node_mapper = json.load(f)
        with open(clique_mapper_filename) as f:
            self.clique_mapper = json.load(f)
        with open(data_filename) as f:
            for item in f:
                self.data.append(json.loads(item))
 
    def test_genNodes_nx(self):
        g = nx.MultiGraph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data)
        self.assertTrue(nx.number_of_nodes(g), 3)
        # get node with key.
        key1 = ('a_val2_2',)
        key2 = ('c_val1_2',)
        self.assertTrue(key1 in g.nodes)
        # print(g.node[key1])
        # print(g.node[key2])

    def test_genNodes_ig(self):
        g = ig.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data)
        self.assertTrue(g.vcount, 3)
        # get node with key.
        key1 = "('a_val2_2',)"
        key2 = "('c_val1_2',)"
        x = None 
        try: x = g.vs.find(str(key1)) # ids are stored as strings in igraph
        except: pass
        self.assertTrue(x['name'] == str(key1))

    def test_genClique_nx(self):
        g = nx.MultiGraph()
        g = graphgen.create_graph(g,
                graph_mapper = self.clique_mapper,
                data_provider = self.data)
        self.assertTrue(nx.number_of_edges(g), 3)
        # locate an edge
        key1 = ('a_val2_1',)
        key2 = ('c_val1_1',)
        self.assertTrue(g.has_node(key1))
        self.assertTrue(key2 in g)
        self.assertTrue(g.has_edge(key1, key2))
        key3 = ('ABCDEF',) # invalid node key
        self.assertFalse(key3 in g)
        self.assertFalse(g.has_edge(key1, key3))

    def test_genClique_ig(self):
        g = ig.Graph()
        g = graphgen.create_graph(g,
                graph_mapper = self.clique_mapper,
                data_provider = self.data)
        self.assertTrue(g.ecount(), 3)
        # locate an edge
        key1 = ('a_val2_1',)
        key2 = ('c_val1_1',)
  
        v1 = None 
        try: v1 = g.vs.find(str(key1)) # ids are stored as strings in igraph
        except: pass
        self.assertTrue(v1['name'] == str(key1))
        v2 = None 
        try: v2 = g.vs.find(str(key2)) # ids are stored as strings in igraph
        except: pass
        self.assertTrue(v2['name'] == str(key2))
        e = None
        try:  e = g.es.select(_between=([v1], [v2]))
        except: pass
        self.assertTrue(e != None)
        key3 = ('ABCDEF',) # invalid node key
        v3 = None 
        try: v3 = g.vs.find(str(key3)) # ids are stored as strings in igraph
        except: pass
        self.assertTrue(v3 == None)
        e = None
        try:  e = g.es.find(_between(v1, v3))
        except: pass
        self.assertTrue(e == None)

# if __name__ == '__main__':
#     unittest.main()
