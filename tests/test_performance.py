import unittest
import networkx as nx
import time
import random
import string
from graphene import graphgen

def randomString(stringLength = 10):
    """Generate a random string with the combination of lowercase and uppercase letters """
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(stringLength))


class TestPerformance(unittest.TestCase):

    def setUp(self):
        self.node_mapper = {
            'nodes': [
                {
                    'type': 'TypeA',
                    'path': '/',
                    'key' : [
                        {'name': 'attr2', 'raw': '/type_a_attr2'}
                        ],
                    'attributes': [
                        {'name': 'attr1', 'raw': '/type_a_attr1'},
                        {'name': 'attr2', 'raw': '/type_a_attr2'}
                        ]
                },
                {
                    'type': 'TypeB',
                    'path': '/path1',
                    'key' : [
                        {'name': 'attr1', 'raw': '/path1/type_b_attr'}
                        ]
                },
                {
                    'type': 'TypeC',
                    'path': '/path1/path2',
                    'key' : [
                        {'name': 'attr1', 'raw': '/path1/path2/type_c_attr'}
                        ]
                },
            ]               
        }
        self.clique_mapper = {
            'cliques': [
                {
                    'type': '__default__',
                    'nodes': [
                        {
                            'type': 'TypeA',
                            'path': '/',
                            'key' : [
                                {'name': 'attr2', 'raw': '/type_a_attr2'}
                                ],
                        },
                        {
                            'type': 'TypeB',
                            'path': '/path1',
                            'key' : [
                                {'name': 'attr1', 'raw': '/path1/type_b_attr'}
                                ]
                        },
                        {
                            'type': 'TypeC',
                            'path': '/path1/path2',
                            'key' : [
                                {'name': 'attr1', 'raw': '/path1/path2/type_c_attr'}
                                ]
                        },
                    ]   
                }
            ]
        }

    @classmethod
    def setUpClass(cls):
        cls.num_of_elements = 100000
        cls.data = []
        for i in range(cls.num_of_elements):
            obj = {
                "type_a_attr1" : "type_a_" + randomString(),
                "type_a_attr2" : "type_a_" + randomString(),
                "path1" : {
                    "type_b_attr" : "type_b_" + randomString(),
                    "path2" : {
                        "type_c_attr" : "type_c_" + randomString(),
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
        print('# edges: {} - duration:{:.2f} sec'.format(g.number_of_nodes(), duration))
        self.assertTrue(duration < 1)
    
    def test_genClique(self):
        g = nx.Graph()
        start = time.time()
        g = graphgen.create_graph(g,
                graph_mapper = self.clique_mapper,
                data_provider = self.data)
        end = time.time()
        duration = end - start
        print('# nodes: {} - duration:{:.2f} sec'.format(g.number_of_edges(), duration))
        self.assertTrue(duration < 1)

# if __name__ == '__main__':
#     unittest.main()
