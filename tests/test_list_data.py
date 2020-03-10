import os
import unittest
import json
import networkx as nx
from graphene import graphgen

mapper_filename   = './resources/list_mapper.json'
data_filename     = './resources/test_list_data.txt'

class TestLisData(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.clique_mapper = None
        self.data = []
        with open(mapper_filename) as f:
            self.mapper = json.load(f)
        with open(data_filename) as f:
            for item in f:
                self.data.append(json.loads(item))

    def test_genGraph(self):
        g = nx.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.mapper, 
                data_provider = self.data, add_type_to_key = True)
        # print('\nNODES1:', g.nodes(data = True))
        self.assertEqual(nx.number_of_nodes(g), 9)
        self.assertEqual(nx.number_of_edges(g), 10)
        # get node with key.
        key1 = ('TypeA', 'a_val1_1')
        key2 = ('TypeB', 'b_Text_2')
        self.assertTrue(key1 in g.nodes)
        self.assertTrue(key2 in g.nodes)
        # print(g.node[key1])
        # print(g.node[key2])
        # check eges with data
        self.assertTrue(g.has_edge(key1, key2))
        edge_data = g.get_edge_data(key1, key2)
        # print('e1:', edge_data)
        self.assertTrue(edge_data != {})
        key3 = ('TypeA', '_UNKNOWN_') # we shouldn't have that
        self.assertFalse(key3 in g)
        key4 = ('TypeB', '_UNKNOWN_') # we shouldn't have that
        self.assertFalse(key4 in g)
 

# if __name__ == '__main__':
#     unittest.main()
