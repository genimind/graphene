import os
import unittest
import networkx as nx
import time
import random
import string
import json
from graphene import graphgen

node_mapper_filename   = './node_mapper.json'
clique_mapper_filename = './clique_mapper.json'

def randomString(stringLength = 10):
    """Generate a random string with the combination of lowercase and uppercase letters """
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


class TestPerformance(unittest.TestCase):

    def setUp(self):
        self.node_mapper = None
        self.clique_mapper = None
        with open(node_mapper_filename) as f:
            self.node_mapper = json.load(f)
        with open(clique_mapper_filename) as f:
            self.clique_mapper = json.load(f)

    @classmethod
    def setUpClass(cls):
        cls.num_of_elements = 100000
        cls.data = []
        for i in range(cls.num_of_elements):
            obj = {
                "a_attr1" : "a_" + randomString(),
                "a_attr2" : "a_" + randomString(),
                "path_b" : {
                    "b_attr1" : "b_" + randomString(),
                    "path_c" : {
                        "c_attr1" : "c_" + randomString(),
                        }
                    } 
            }
            cls.data.append(obj)

    def test_genNodes(self):
        g = nx.Graph()
        start = time.time()
        g = graphgen.create_graph(g, 
                graph_mapper = self.node_mapper, 
                data_provider = self.data)
        end = time.time()
        duration = end - start
        print('# nodes: {} - duration:{:.2f} sec'.format(g.number_of_nodes(), duration))
        self.assertTrue(duration < 3)
    
    def test_genClique(self):
        g = nx.Graph()
        start = time.time()
        g = graphgen.create_graph(g,
                graph_mapper = self.clique_mapper,
                data_provider = self.data)
        end = time.time()
        duration = end - start
        print('# edges: {} - duration:{:.2f} sec'.format(g.number_of_edges(), duration))
        self.assertTrue(duration < 4)

# if __name__ == '__main__':
#     unittest.main()
