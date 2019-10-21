import os
import unittest
import json
import networkx as nx
from graphene import graphgen

node_mapper_filename   = './node_deep_mapper.json'
data_filename          = './test_deep_data.txt'

class TestDeepData(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.clique_mapper = None
        self.data = []
        with open(node_mapper_filename) as f:
            self.node_mapper = json.load(f)
        with open(data_filename) as f:
            for item in f:
                self.data.append(json.loads(item))

    def test_genDeepNodes(self):
        g = nx.Graph()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data, add_type_to_key = True)
        self.assertEqual(nx.number_of_nodes(g), 15)
        # get node with key.
        key1 = ('TypeA', 'a_val2_1')
        key2 = ('TypeB', 'b_val2_21', 'b_val1_21')
        self.assertTrue(key1 in g.nodes)
        self.assertTrue(key2 in g.nodes)
        print(g.node[key1])
        print(g.node[key2])
        keyU = ('TypeA', '_UNKNOWN_')
        self.assertFalse(keyU in g.nodes)


# if __name__ == '__main__':
#     unittest.main()
