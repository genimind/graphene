import os
import unittest
import json
import networkx as nx
from graphene import graphgen

node_mapper_filename   = './node_mapper.json'
edge_mapper_filename   = './edge_mapper.json'
data_filename          = './test_data.txt'

class TestReadEdges(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.edge_mapper = None
        self.data = []
        with open(node_mapper_filename) as f:
            self.node_mapper = json.load(f)
        with open(edge_mapper_filename) as f:
            self.edge_mapper = json.load(f)
        with open(data_filename) as f:
            for item in f:
                self.data.append(json.loads(item))

    def test_genEdges(self):
        g = nx.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.edge_mapper, 
                data_provider = self.data, add_type_to_key = True)
        # print(g.edges)
        self.assertEqual(nx.number_of_nodes(g), 15)
        self.assertEqual(nx.number_of_edges(g), 10)
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
    
    def test_genNodesAndEdges(self):
        g = nx.Graph()
        g = graphgen.create_graph(g,
                graph_mapper = self.node_mapper,
                data_provider = self.data, add_type_to_key = True)
        g = graphgen.create_graph(g,
                graph_mapper= self.edge_mapper,
                data_provider= self.data, add_type_to_key= True)       
        self.assertEqual(nx.number_of_nodes(g), 15)
        self.assertEqual(nx.number_of_edges(g), 10)
        # locate an edge
        key1 = ('TypeA', 'a_val2_3')
        key2 = ('TypeB', 'b_val2_3', 'b_val1_3')
        self.assertTrue(g.has_node(key1))
        self.assertTrue(g.has_node(key2))
        self.assertTrue(key2 in g)
        self.assertTrue(g.has_edge(key1, key2))
        # check node data
        node_data = g.nodes[key1]
        self.assertTrue(node_data != {})
        # print('node_data:', node_data)
        # check edge data
        edge_data = g.get_edge_data(key1, key2)
        self.assertTrue(edge_data != {})
        # the graph is bidirectional
        self.assertTrue(g.has_edge(key2, key1))
        # print('edge:', g.edges[(key2, key1)])


# if __name__ == '__main__':
#     unittest.main()
