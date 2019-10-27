import os
import unittest
import json
import networkx as nx
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
 
    def test_genNodes(self):
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
    
    def test_genClique(self):
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


# if __name__ == '__main__':
#     unittest.main()
