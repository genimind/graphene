# Copyright 2019-2020 Genimind Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
import networkx as nx
import igraph as ig

class GraphRep:

    class GraphBase(ABC):
        @abstractmethod
        def add_node(self, id, type, **attr):
            pass

        @abstractmethod
        def add_edge(self, id, type, **attr):
            pass

    class GraphNX(GraphBase):
        def __init__(self, graph: nx.Graph):
            self._graph = graph

        def add_node(self, id, node_type, **attr):
            attr['_type_'] = node_type
            self._graph.add_node(str(id), **attr)

        def add_edge(self, from_id, to_id, edge_type, **attr):
            attr['_type_'] = edge_type
            self._graph.add_edge(str(from_id), str(to_id), **attr)

    class GraphIG(GraphBase):
        def __init__(self, graph: ig.Graph):
            self._graph = graph

        def add_node(self, id, node_type, **attr):
            attr['_type_'] = node_type
            self._graph.add_vertex(name=str(id), **attr)
            
        def add_edge(self, id, edge_type, **attr):
            attr['_type_'] = edge_type
            self._graph.add_edge(str(from_id), str(to_id), **attr)


    def __init__(self, graph):
        # based on the graph we'll map the ndoe creation to it.
        if isinstance(graph, nx.Graph):
            self._graph = self.GraphNX(graph)
        elif isinstance(graph, ig.Graph):
            self._graph = self.GraphIG(graph)
        else:
            raise("Unsupported graph type. We only accept NetworkX and iGraph")

    def add_node(self, id, data):
        return self._graph.add_node(id, data)

    def add_edge(self, id, data):
        return self._graph.add_edge(id, data)

    @property
    def graph(self):
        return self._graph._graph