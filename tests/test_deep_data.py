import os
import unittest
import json
import networkx as nx
from graphene import graphgen

node_mapper_filename   = './node_deep_mapper.json'
edge_mapper_filename   = './edge_deep_mapper.json'
data_filename          = './test_deep_data.txt'

class TestDeepData(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.clique_mapper = None
        self.data = []
        with open(node_mapper_filename) as f:
            self.node_mapper = json.load(f)
        with open(edge_mapper_filename) as f:
            self.edge_mapper = json.load(f)
        with open(data_filename) as f:
            for item in f:
                self.data.append(json.loads(item))

    def test_genDeepNodes(self):
        g = nx.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data, add_type_to_key = True)
        # print('\nNODES1:', g.nodes(data = True))
        self.assertEqual(nx.number_of_nodes(g), 21)
        # get node with key.
        key1 = ('TypeA', 'a_val2_1')
        key2 = ('TypeB', 'b_val2_21', 'b_val1_21')
        self.assertTrue(key1 in g.nodes)
        self.assertTrue(key2 in g.nodes)
        # print(g.node[key1])
        # print(g.node[key2])
        keyU = ('TypeA', '_UNKNOWN_')
        self.assertFalse(keyU in g.nodes)

    def test_genEdges(self):
        g = nx.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.edge_mapper, 
                data_provider = self.data, add_type_to_key = True)
        # print('\nNODES2:', g.nodes(data = True))
        self.assertEqual(nx.number_of_nodes(g), 21)
        # print('\nEDGES2:', g.edges())
        self.assertEqual(nx.number_of_edges(g), 38)
        # get node with key.
        key1 = ('TypeA', 'a_val2_1')
        key2 = ('TypeB', 'b_val2_1', 'b_val1_1')
        key3 = ('TypeB', 'b_val2_3', 'b_val1_3')
        key4 = ('TypeC', 'c_val1_3')
        self.assertTrue(key1 in g.nodes)
        self.assertTrue(key2 in g.nodes)
        # print(g.node[key1])
        # print(g.node[key2])
        # check eges with data
        self.assertTrue(g.has_edge(key1, key2))
        edge_data = g.get_edge_data(key1, key2)
        self.assertTrue(edge_data != {})
        # print('e1:', edge_data)
        self.assertTrue(g.has_edge(key3, key4))
        edge_data = g.get_edge_data(key3, key4)
        self.assertTrue(edge_data != {})
        # print('e2:', edge_data)
        key5 = ('TypeC', 'ABCDEF') # invalid node key
        self.assertFalse(key5 in g)
        self.assertFalse(g.has_edge(key2, key5))
 

# if __name__ == '__main__':
#     unittest.main()
