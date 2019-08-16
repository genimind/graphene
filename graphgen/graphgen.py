#
# code that generate the graph (networkx) and other utilities
#

import pandas as pd

#from enum import Enum
#
#class GraphEdges(Enum):
#    UNIDIR = 1
#    BIDIR  = 2
#    

def check_attributes(the_type, raw_data, attributes):
    # get list of raw_data columns (raw_data is a pandas dataframe)
    columns = raw_data.columns
    for attr in attributes:
        if attr['raw'] not in columns:
            print("mismatch between raw data and mapper: \n \
                 - {} \n \
                 - columns: {} \n \
                 - attribute: {}".format(the_type, columns.values, attr['raw']))
            return False
    return True

def create_graph(graph, graph_mapper, data_provider, update = True):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: dataframe where attributes and graph objects are created from
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''
#    # graph object
#    gObj = None
#
#    if graph_edges == GraphEdges.BIDIR:
#        gObj = nx.MultiDiGraph()
#    else:
#        gObj = nx.MultiGraph()
#        
    mapper_types = ['nodes', 'edges', 'cliques']
    assert (graph != None),"Graph object wasn't constructed correctly"
    assert (any(x in mapper_types for x in graph_mapper.keys())), "Graph mapper is missing one of the mapper types: {}".format(mapper_types)
            
    if isinstance(data_provider, pd.DataFrame):
        graph = create_graph_from_pandas(graph, graph_mapper, data_provider, update)
        
    elif isinstance(data_provider, list) and isinstance(data_provider[0], dict):
        # we have a list of dict object, which could be from JSON file reader.
        graph = create_graph_from_json(graph, graph_mapper, data_provider, update)
    
    return graph




def create_graph_from_pandas(graph, graph_mapper, data_provider, update = True):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: dataframe where attributes and graph objects are created from
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of node types and edge types
    node_types = []
    edge_types = []

    if 'nodes' in graph_mapper.keys():
        node_types = graph_mapper['nodes']
    if 'edges' in graph_mapper.keys():
        edge_types = graph_mapper['edges']

    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_type in node_types:
        assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        node_key = node_type['key'][0]
        id_name = node_key['name']
        id_raw_name = raw_data.columns.get_loc(node_key['raw'])

        attr_dict = {}
        for a in node_type['attributes']:
            attr_dict[a['name']] = raw_data.columns.get_loc(a['raw'])

        attr = dict()
        # count = 0
        node_type_name = node_type['type']
        for row in raw_data.itertuples(index = False):
            node_id = '{}_{}'.format(node_type_name, row[id_raw_name])
            if not update and graph.has_node(node_id):
                continue

            attr['_type_'] = node_type_name
            for k,v in attr_dict.items():
                attr[k] = row[v]
            graph.add_node(node_id, **attr)
            # count += 1
        # print(count)

        
    for edge_type in edge_types:
        
        assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        src = edge_type['from']['key'][0]['raw']
        src_type_name = edge_type['from']['type']
        src_index = raw_data.columns.get_loc(src)

        dst = edge_type['to']['key'][0]['raw']
        dst_index = raw_data.columns.get_loc(dst)
        dst_type_name = edge_type['to']['type']

        attr_dict = {}
        for a in edge_type['attributes']:
            attr_dict[a['name']] = raw_data.columns.get_loc(a['raw'])
        
        attr = dict()
        for row in raw_data.itertuples(index = False):
            attr['_type_'] = edge_type['type']
            for k,v in attr_dict.items():
                attr[k] = row[v]
            from_id = '{}_{}'.format(src_type_name, row[src_index])
            to_id   = '{}_{}'.format(dst_type_name, row[dst_index])
            graph.add_edge(from_id, to_id, **attr)
        
        
    return graph


def create_graph_from_json(graph, graph_mapper, data_provider, update = True):
    '''
    
    Nodes in the graph will have an attribute '_id_' that was originally the key in the source data.
    
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: dataframe where attributes and graph objects are created from
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (isinstance(data_provider, list) and isinstance(data_provider[0], dict)), "The data provider should be a list of dict"
    
    # get list of node types and edge types
    node_types = []
    edge_types = []

    if 'nodes' in graph_mapper.keys():
        graph = create_graph_nodes_from_json(graph, graph_mapper, data_provider, update)
    if 'edges' in graph_mapper.keys():
        graph = create_graph_edges_from_json(graph, graph_mapper, data_provider, update)
    if 'cliques' in graph_mapper.keys():
        graph = create_graph_clique_from_json(graph, graph_mapper, data_provider, update)
            
    return graph


def extract_node_attrs_from_json(jdata, type_path, attr_list):
#     print('>>> looking for:', type_path)
#     print('>>> looking for attrs:', attr_list)
    out = []

    # make sure our type_path end with '/'
    if type_path[-1] != '/':
        type_path += '/' 
    
    
    def extract_data(jdata, cur_path = '/', cur_obj = None):
        if type(jdata) is dict:            
            if cur_path == type_path:
#                 print('<<< MATCHED_TYPE >>>')
#                 print('@path:', cur_path)
                obj = {}
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', obj)
#                 print('>>> got obj', obj)
                out.append(obj)
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/')
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path)
        else:
#             print('cur_path: {} - type_path: {}'.format(cur_path, type_path))
            if cur_obj != None and cur_path in attr_list:
#                 print('<<< MATCHED_ATTR >>>')
                cur_obj[cur_path] = jdata
    
    extract_data(jdata)
    return out
                
                
def create_graph_nodes_from_json(graph, graph_mapper, data_provider, update = True):
    '''
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of node types and edge types
    node_types = []
 
    if 'nodes' in graph_mapper.keys():
        node_types = graph_mapper['nodes']
 
    raw_data = data_provider
    
#     print(node_types)
#     print(edge_types)
    
    for node_type in node_types:
        # TBD... assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
#         print('node_type to process:', node_type)
        node_key = node_type['key'][0]
        key_name = node_key['name']
        key_raw_name = node_key['raw']

        attr_dict = {}
        if 'attributes' in node_type:
            for a in node_type['attributes']:
                raw_attr = a['raw']
                if raw_attr[-1] != '/':
                    raw_attr += '/'
                attr_dict[a['name']] = raw_attr
        
        attr = dict()
        count = 0
        node_type_name = node_type['type']
        node_type_path = node_type['path']
        if node_type_path[-1] != '/':
            node_type_path += '/' 
 
        # construct attribute mapping between raw_attrib_name -> attrib_name
        lookup_attr_list = []
    
        for k, v in attr_dict.items():
            if v[-1] != '/':
                v += '/'
            lookup_attr_list.append(v)
        
        # make sure we have the key_raw_name in the list of attributes
        if key_raw_name[-1] != '/':
            key_raw_name += '/'
        if key_raw_name not in lookup_attr_list:
            lookup_attr_list.append(key_raw_name)
        # print('lookup_attr_list:', lookup_attr_list)
        
        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            jelem = extract_node_attrs_from_json(j, node_type_path, lookup_attr_list)
            if len(jelem) > 0:
                for e in jelem:
                    # print('{} - type_found: {} - attr: {}'.format(count, node_type_name, e))
                    key_value = e[key_raw_name] if key_raw_name in e else 'UNKNOWN_'+str(count)
                    node_id = '{}_{}'.format(node_type_name, key_value)
                    if not update and graph.has_node(node_id):
#                         print('graph has node', node_id)
                        continue

                    attr['_type_'] = node_type_name
                    for k,v in attr_dict.items():
                        attr[k] = e[v] if v in e else ''
#                     print('>> adding node: ', node_id)
                    graph.add_node(node_id, **attr)
                    count += 1
        
        # print('type: {} - {}'.format(node_type_path, count))
        
    return graph
            

            
def extract_edge_attrs_from_json(jdata, src_type_path, dst_type_path, attr_list):
#     print('>>> looking for:', type_path)
#     print('>>> looking for attrs:', attr_dict)
    out = []

    # make sure our type_path end with '/'
    if src_type_path[-1] != '/':
        src_type_path += '/' 
    
    if dst_type_path[-1] != '/':
        dst_type_path += '/' 
        
    def extract_data(jdata, cur_path = '/', cur_obj = None, got_from = False, got_to = False):
        if type(jdata) is dict:            
            if cur_path == src_type_path or cur_path == dst_type_path:
                if cur_path == src_type_path:
                    got_from = True
                else:
                    got_to = True
#                 print('<<< MATCHED_TYPE >>>')
#                 print('@path:', cur_path)
                obj = {}
                if cur_obj != None: # we're still collecting
                    obj = cur_obj
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', obj, got_from, got_to)
#                     print('>>> got edge data:', obj)
#                 print('got_from:', got_from, ' - got_to:', got_to)
                if got_from and got_to:
                    out.append(obj)
                    got_from = got_to = False
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, got_from, got_to)
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path, cur_obj, got_from, got_to)
        else:
#             print('cur_path: {} - src_type_path: {} - dst_type_path: {}'.format(cur_path, 
#                                                                                 src_type_path, dst_type_path))
            if cur_obj != None and cur_path in attr_list:
#                 print('<<< MATCHED_ATTR >>>')
                cur_obj[cur_path] = jdata

    extract_data(jdata)
    return out
                
            
def create_graph_edges_from_json(graph, graph_mapper, data_provider, update = True):
    '''
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of edge types and edge types
    edge_types = []

    if 'edges' in graph_mapper.keys():
        edge_types = graph_mapper['edges']

    raw_data = data_provider
    
#     print(edge_types)
    for edge_type in edge_types:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        edge_type_name = edge_type['type']

        # source node metadata
        src_type_name = edge_type['from']['type']
        src_type_path = edge_type['from']['path']
        if src_type_path[-1] != '/':
            src_type_path += '/' 

        src_key = edge_type['from']['key'][0]
        src_key_name = src_key['name']
        src_key_raw_name = src_key['raw']
        if src_key_raw_name[-1] != '/':
            src_key_raw_name += '/'
        
        # destination node metadata
        dst_type_name = edge_type['to']['type']
        dst_type_path = edge_type['to']['path']
        if dst_type_path[-1] != '/':
            dst_type_path += '/' 

        dst_key = edge_type['to']['key'][0]
        dst_key_name = dst_key['name']
        dst_key_raw_name = dst_key['raw']
        if dst_key_raw_name[-1] != '/':
            dst_key_raw_name += '/'
    

        attr_dict = {}
        if 'attributes' in edge_type:
            for a in edge_type['attributes']:
                v = a['raw']
                if v[-1] != '/':
                    v += '/'
                attr_dict[a['name']] = v
            
        attr = dict()
        count = 0
 
        # construct attribute mapping between raw_attrib_name_path -> attrib_name
        lookup_attr_list = []
    
        # TBD: review this code, not sure if it's necessary
        for k, v in attr_dict.items():
            lookup_attr_list.append(v)


        # make sure we have the src_key_raw_name in the list of attributes
        if src_key_raw_name not in lookup_attr_list:
            lookup_attr_list.append(src_key_raw_name)
            
        # make sure we have the dst_key_raw_name in the list of attributes
        if dst_key_raw_name not in lookup_attr_list:
            lookup_attr_list.append(dst_key_raw_name)

#         print('lookup_attr_list:', lookup_attr_list)

        
        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            jelem = extract_edge_attrs_from_json(j, src_type_path, dst_type_path, lookup_attr_list)
            if len(jelem) > 0:
                for e in jelem:
#                     print('{} - src: {} - dest: {} - attr: {}'.format(count, src_type_name, dst_type_name, e))
                    src_key_value = e[src_key_raw_name] if src_key_raw_name in e else 'UNKNOWN_'+str(count)
                    dst_key_value = e[dst_key_raw_name] if dst_key_raw_name in e else 'UNKNOWN_'+str(count)
        
                    attr['_type_'] = edge_type_name
                    for k,v in attr_dict.items():
                        attr[k] = e[v] if v in e else ''
                    from_id = '{}_{}'.format(src_type_name, src_key_value)
                    to_id = '{}_{}'.format(dst_type_name, dst_key_value)
#                     print('adding edge from: {} -> to: {}, attr: {}'.format(from_id, to_id, attr))
                    graph.add_edge(from_id, to_id, **attr)
                    
                    count += 1
        
        print('type: {} -> {} - {}'.format(src_type_path, dst_type_path, count))
       
    return graph
            


def extract_clique_attrs_from_json(jdata, node_list, attr_list):
    '''
        For now we'll assume that all attributes are available and there are no
        multiple group of cliques within the same group. We might need to revist
        this later (TBD)
    '''
    print('>>> looking for attrs:', attr_list)
    out = []

    def extract_data(jdata, cur_path = '/', cur_obj = None):
        if type(jdata) is dict:            
            for a in jdata:
                extract_data(jdata[a], cur_path + a + '/', cur_obj)
                if all(x in attr_list for x in cur_obj.keys()): # we got all attributres
                    out.append(cur_obj)
                    cur_obj = {}
        elif type(jdata) is list:
            for a in jdata:
                extract_data(a, cur_path, cur_obj)
        else:
            print('cur_path: {} '.format(cur_path))
            if cur_obj != None and cur_path in attr_list:
                print('<<< MATCHED_ATTR >>>', cur_path)
                cur_obj[cur_path] = jdata

    extract_data(jdata, cur_obj={})
    return out
 

def create_graph_clique_from_json(graph, graph_mapper, data_provider, update=True):

    '''
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''


    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of edge types and edge types
    cliques = []

    if 'cliques' in graph_mapper.keys():
        cliques = graph_mapper['cliques']

    raw_data = data_provider
    
#     print(cliques)
    for clique in cliques:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        clique_type_name = clique['type']

        # clique nodes metadata
        node_cliques = clique['nodes']
        node_list = []

        # collect node information needed to pick data.
        for node_type in node_cliques:
            # TBD... assert check_attributes(node_type, raw_data, node_type['attributes'])
        
            # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
    #         print('node_type to process:', node_type)
            node_meta = {}
            node_meta['node_type'] = node_type['type']
            node_meta['node_path'] = node_type['path']
            node_meta['key_path'] = node_type['key'][0]['raw'] # TBD: we only support single key
            if node_meta['node_path'][-1] != '/':
                node_meta['node_path'] += '/'
            if node_meta['key_path'][-1] != '/':
                node_meta['key_path'] += '/'

            attr_dict = {}
            if 'attributes' in node_type:
                for a in node_type['attributes']:
                    raw_attr = a['raw']
                    if raw_attr[-1] != '/':
                        raw_attr += '/'
                    attr_dict[a['name']] = raw_attr
            node_meta['attributes'] = attr_dict
            
            # construct attribute mapping between raw_attrib_name -> attrib_name
            lookup_attr_list = []
        
            for k, v in attr_dict.items():
                if v[-1] != '/':
                    v += '/'
                lookup_attr_list.append(v)

           if node_meta['key_path'] not in lookup_attr_list:
                lookup_attr_list.append(node_meta['key_path'])

            node_meta['lookup_attr_list'] = lookup_attr_list            

            # print('node_meta:', node_meta)
            node_list.append(node_meta)
            
        attr = dict()
        count = 0

        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            for node in node_list:
                jelem = extract_node_attrs_from_json(j, node['node_path'], node['lookup_attr_list'])
                node['extracted_elem'] = jelem

            # construct the clique
            for node in node_list:
                if len(jelem) > 0:
                    extracted_elems += jelem
                for e in jelem:
                    # print('{} - type_found: {} - attr: {}'.format(count, node_type_name, e))
                    key_value = e[key_raw_name] if key_raw_name in e else 'UNKNOWN_'+str(count)
                    node_id = '{}_{}'.format(node_type_name, key_value)
                    if not update and graph.has_node(node_id):
#                         print('graph has node', node_id)
                        continue

                    attr['_type_'] = node_type_name
                    for k,v in attr_dict.items():
                        attr[k] = e[v] if v in e else ''
#                     print('>> adding node: ', node_id)
                    graph.add_node(node_id, **attr)
                    count += 1
        
        # print('type: {} - {}'.format(node_type_path, count))
        
    return graph


def create_graph_clique_from_json___olde(graph, graph_mapper, data_provider, update = True):
    '''
    
    params:
        graph: fully constructed graph object to add new nodes and edges to it.
        graph_mapper: dictionary describing the type of object to extract
        data_provider: json_data
        
    return:
        constructured "graph_type" graph object based on the provided source data and according to 
        the mapper schema description.
    '''

    assert (graph != None),"Graph object wasn't constructed correctly"
    # TBD... assert (isinstance(data_provider, pd.DataFrame)),"The data provider should be a pandas DataFrame"
    
    # get list of edge types and edge types
    cliques = []

    if 'cliques' in graph_mapper.keys():
        cliques = graph_mapper['cliques']

    raw_data = data_provider
    
#     print(cliques)
    for clique in cliques:
        
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # TBD: Need to support multiple keys. For now we'll only have a single key for each record 
        clique_type_name = clique['type']

        # clique nodes metadata
        node_clique = clique['nodes']
        node_list = []
        for node_type in node_clique:
            node_meta = {}
            node_meta['node_type'] = node_type['type']
            node_meta['node_path'] = node_type['path']
            node_meta['key_path'] = node_type['key'][0]['raw'] # TBD: we only support single key
            if node_meta['node_path'][-1] != '/':
                node_meta['node_path'] += '/'
            if node_meta['key_path'][-1] != '/':
                node_meta['key_path'] += '/'
            node_list.append(node_meta)
        count = 0
 
        lookup_attr_list = []
        for node_meta in node_list:
            # make sure we have the key path in the list of attributes
            if node_meta['key_path'] not in lookup_attr_list:
                lookup_attr_list.append(node_meta['key_path'])
            
        print('lookup_attr_list:', lookup_attr_list)

        attr = dict()
        
        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            jelem = extract_clique_attrs_from_json(j, node_list, lookup_attr_list)
            if len(jelem) > 0:
                print('jelem', jelem)
                for e in jelem:
                    print('what we got from the extraction:', e)
                    print('num of node_list', len(node_list))
                    for src_node in node_list:
                        for dst_node in node_list:
                            if src_node == dst_node: # no self loop
                                continue
                            src_key_path = src_node['key_path']
                            dst_key_path = dst_node['key_path']
#                           print('{} - src: {} - dest: {} '.format(count, src_type_name, dst_type_name, e))
                            src_key_value = e[src_key_path] if src_key_path in e else None
                            dst_key_value = e[dst_key_path] if dst_key_path in e else None
        
                            attr['_type_'] = clique_type_name
                            # for k,v in attr_dict.items():
                            #     attr[k] = e[v] if v in e else ''
                            if src_key_value and dst_key_value:
                                from_id = '{}_{}'.format(src_node['node_type'], src_key_value)
                                to_id = '{}_{}'.format(dst_node['node_type'], dst_key_value)
                                print('adding edge from: {} -> to: {}, attr: {}'.format(from_id, to_id, attr))
                                graph.add_edge(from_id, to_id)
                    
                            count += 1
        
                            print('clique: {} -> {} - {}'.format(src_node['node_type'], dst_node['node_type'], count))
       
    return graph
             
