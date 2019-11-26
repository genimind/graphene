#
# code that generate the graph (networkx) and other utilities
#

import pandas as pd
import re
from .process_utilities import *
from .process_json import ProcessJsonData

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

def create_graph(graph, graph_mapper, data_provider, add_type_to_key = False, update = True):
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
        graph = create_graph_from_pandas(graph, graph_mapper, data_provider, add_type_to_key, update)
        
    elif isinstance(data_provider, list) and isinstance(data_provider[0], dict):
        # we have a list of dict object, which could be from JSON file reader.
        graph = create_graph_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    
    return graph




def create_graph_from_pandas(graph, graph_mapper, data_provider, add_type_to_key, update = True):
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
            node_id = (node_type_name, str(row[id_raw_name]))
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
            from_id = (src_type_name, str(row[src_index]))
            to_id   = (dst_type_name, str(row[dst_index]))
            graph.add_edge(from_id, to_id, **attr)
        
        
    return graph


def create_graph_from_json(graph, graph_mapper, data_provider, add_type_to_key, update = True):
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
    # node_types = []
    # edge_types = []

    if 'nodes' in graph_mapper.keys():
        graph = create_graph_nodes_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    if 'edges' in graph_mapper.keys():
        graph = create_graph_edges_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
    if 'cliques' in graph_mapper.keys():
        graph = create_graph_clique_from_json(graph, graph_mapper, data_provider, add_type_to_key, update)
            
    return graph


def extract_node_attrs_from_json(jdata, node_info):
    type_path = node_info['path']
    attr_list = node_info['lookup_attr_list']
    is_relative = node_info['is_relative']
    is_list = node_info['is_list']

    # print('>>> looking for:', type_path)
    # print('>>> looking for attrs:', attr_list)
    # print('>>> is_relative', is_relative)
    # print('>>> is_list', is_list)

    out = []

    def extract_data(jdata, cur_path = '/', cur_obj = None, collect_data = False):
        if type(jdata) is dict:            
            if valid_path_to_pick(cur_path, type_path, is_relative):
                collect_data = True
                # print('<<< MATCHED_TYPE >>>')
                # print('@path:', cur_path)
                cur_obj = {}
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
                # print('>>> got obj', cur_obj)
                out.append(cur_obj)
                collect_data = False
            # elif collect_data:
            #     for a in jdata:
            #         extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
            else:
                for a in jdata:
                    extract_data(jdata[a], cur_path + a + '/', cur_obj, collect_data)
        elif type(jdata) is list:
            # print('LIST >> cur_path: {} - type_path: {}'.format(cur_path, type_path))
            if is_list and valid_path_to_pick(cur_path, type_path, is_relative):
                # TBD: Here we only support one type of item which is __item__ and only 
                #      one. Note usre if this will cover othe cases
                attr = attr_list[0]
                for a in jdata:
                    # print('list_item:', a)
                    collect = True
                    if attr[1] != None and not re.match(attr[1], a):
                        collect = False
                    if collect:
                        cur_obj = {}
                        cur_obj[attr] = a 
                        out.append(cur_obj)
            else: # just normal processing.
                for a in jdata:
                    extract_data(a, cur_path, cur_obj, collect_data)
        else:
            # print('cur_path: {} - type_path: {}'.format(cur_path, type_path))
            # TBD: support patterns for normal attributes
            if cur_obj != None:
                attr = attr_in_attrlist(cur_path, attr_list, is_relative)
                if attr != None:
                    # print('<<< MATCHED_ATTR >>>')
                    cur_obj[attr] = jdata
    
    extract_data(jdata)
    return out
                
                
def create_graph_nodes_from_json(graph, graph_mapper, 
                                 data_provider, 
                                 add_type_to_key, 
                                 update = True):
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
    
    # get list of node types and their attributes
    node_list = []
 
    nodes = graph_mapper['nodes']
 
    raw_data = data_provider
        
    for node_info in nodes:
        # TBD... assert check_attributes(node_type, raw_data, node_type['attributes'])
       
        node_info = configure_node_info(node_info)
 
        attr_dict = {}
        if 'attributes' in node_info:
            for a in node_info['attributes']:
                raw_attr = a['raw']
                raw_attr = correct_path(raw_attr)
                pattern = re.compile(a['pattern']) if 'pattern' in a else None
                attr_dict[a['name']] = (raw_attr, pattern)

        node_info['attr_info'] = attr_dict
        
        # construct attribute mapping between raw_attrib_name -> attrib_name
        lookup_attr_list = list(attr_dict.values())
    
        # make sure we have the key_raw_name in the list of attributes
        lookup_attr_list = append_keys_to_lookup_attributes(node_info, lookup_attr_list)
        node_info['lookup_attr_list'] = lookup_attr_list

        node_list.append(node_info)

    count = 0

    # iterate and collect.  
    for j in raw_data:
        for node in node_list:
            jelem = extract_node_attrs_from_json(j, node)
            node['extracted_elem'] = jelem

        for node in node_list:
            jelem = node['extracted_elem']
            for e in jelem:
                # print('>>----<< {} - node_info: {} - attr: {}'.format(count, node, e))
                node_id = construct_key(node, e, add_type_to_key, '_UNKNOWN_')
                if not update and graph.has_node(node_id):
                    # print('graph has node', node_id)
                    continue

                attr = dict()
                attr['_type_'] = node['type']
                for k,v in node['attr_info'].items():
                    attr[k] = e[v] if v in e else ''
                # print('>>> Adding node: ', node_id)
                graph.add_node(node_id, **attr)
                count += 1
        
        # print('type: {} - {}'.format(node_type_path, count))
        
    return graph
            

            
# def extract_edge_attrs_from_json(jdata, from_info, to_info, attr_list):
    # from_path = from_info['path']
    # from_is_relative = from_info['is_relative']
    # from_is_list = from_info['is_list']
    # to_path = to_info['path']
    # to_is_relative = to_info['is_relative']
    # to_is_list = to_info['is_list']

    # print('>>> looking for from:', from_info['path'])
    # print('>>> from is_relative', from_is_relative)
    # print('>>> from is_list', from_is_list)
    # print('>>> looking for to  :', to_info['path'])
    # print('>>> to is_relative', to_is_relative)
    # print('>>> to is_list', to_is_list)
    # print('>>> looking for attrs:', attr_list)

    # out_list = []
    
    # def extract_data(jdata, cur_path = '/', from_count = 0, to_count = 0):
    #     if type(jdata) is dict: 
    #         print('dict(): cur_path: ', cur_path, '- from_count: ', from_count, ', to_count:', to_count)
    #         valid_from = valid_path_to_pick(cur_path, from_path, from_is_relative)
    #         valid_to   = valid_path_to_pick(cur_path, to_path, to_is_relative)
    #         if valid_from or valid_to:
    #             print('@path:', cur_path, '<<< MATCHED_TYPE >>>')
    #             old_from_count = from_count
    #             old_to_count = to_count
    #             if valid_from:
    #                 from_count += 1
    #             if valid_to:
    #                 to_count += 1
    #             if from_count != to_count:
    #                 # we need a new object to use for collecting based on the original one.
    #                 print('....... creating new object', from_count, to_count)
    #                 if len(out_list) == 0:
    #                     out_list.append({})
    #                 else:
    #                     anObj = out_list[-1].copy() # setup a new object to collect with
    #                     out_list.append(anObj)
    #             print('... continue collecting (2) old, old, new, new', 
    #                     old_from_count, old_to_count, from_count, to_count)
    #             for a in jdata:
    #                 extract_data(jdata[a], cur_path + a + '/', from_count, to_count)
    #         else:
    #             for a in jdata:
    #                 extract_data(jdata[a], cur_path + a + '/', from_count, to_count)
    #         # if len(out_list) > 0:
    #         #     print('...out_list:', out_list)

    #     elif type(jdata) is list:
    #         print('LIST >> cur_path: {} - from_path: {}, to_path: {}'.format(cur_path, from_path, to_path))
    #         if (from_is_list and \
    #             valid_path_to_pick(cur_path, from_path, from_is_relative)) or \
    #             (to_is_list and \
    #                 valid_path_to_pick(cur_path, to_path, to_is_relative)):
    #             # collect the items as nodes.
    #             for a in jdata:
    #                 # print('list_item:', a)
    #                 out_list.append({})
    #                 out_list[-1]['__item__/'] = a # we just use the format specifically as a special case
    #                 # print('>>> collected data (3):', out_list[-1])
                    
    #         else:
    #             for a in jdata:
    #                 extract_data(a, cur_path, from_count, to_count)
    #     else:
    #         # print('cur_path: {} -- from_path: {} -- to_path: {}'.format(cur_path, 
    #         #                                                                    from_path, to_path))
    #         attr = attr_in_attrlist(cur_path, attr_list, from_is_relative or to_is_relative)
    #         if attr != None:
    #             # print('@path:', cur_path, '<<< MATCHED_ATTR >>>')
    #             out_list[-1][attr] = jdata
   
    # extract_data(jdata)
    # return out_list
                
            
def create_graph_edges_from_json(graph, graph_mapper, data_provider, add_type_to_key, update = True, verbose = False):
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
    
    # get list of edge types and attributes
    edge_list = []

    edges = graph_mapper['edges']

    raw_data = data_provider
    
    for edge_info in edges:       
        # TBD... assert check_attributes(edge_type, raw_data, edge_type['attributes'])

        # source node metadata
        edge_info['from'] = configure_node_info(edge_info['from'])
        
        # destination node metadata
        edge_info['to'] = configure_node_info(edge_info['to'])

        from_attr_dict = {}
        to_attr_dict = {}
        # Process the attributes and add them to either the from_attr_dict or the 
        # to attr_dict
        if 'attributes' in edge_info:
            for a in edge_info['attributes']:
                raw_attr = a['raw']
                raw_attr = correct_path(raw_attr)
                pattern = re.compile(a['pattern']) if 'pattern' in a else None
                if raw_attr.startswith(edge_info['from']['path']):
                    from_attr_dict[a['name']] = (raw_attr, pattern)
                else:
                    to_attr_dict[a['name']] = (raw_attr, pattern)
            
        edge_info['from']['attr_info'] = from_attr_dict
        edge_info['to']['attr_info'] = to_attr_dict
        
 
        # construct attribute mapping between raw_attrib_name_path -> attrib_name
        from_lookup_attr_list = list(from_attr_dict.values())
        to_lookup_attr_list = list(to_attr_dict.values())

        # make sure we have the src_key_raw_name, and dest_key_raw_name in the list of attributes
        from_lookup_attr_list = append_keys_to_lookup_attributes(edge_info['from'], from_lookup_attr_list)
        to_lookup_attr_list = append_keys_to_lookup_attributes(edge_info['to'], to_lookup_attr_list)

        edge_info['from']['lookup_attr_list'] = from_lookup_attr_list
        edge_info['to']['lookup_attr_list'] = to_lookup_attr_list

        edge_list.append(edge_info)
        
    count = 0

    # iterate and collect.  
    for j in raw_data:
        for edge in edge_list:
    #       print('json>> ', j)
            jelem = extract_node_attrs_from_json(j, edge['from'])
            edge['from']['extracted_elem'] = jelem
            jelem = extract_node_attrs_from_json(j, edge['to'])
            edge['to']['extracted_elem'] = jelem

        
        for edge in edge_list:
            extracted_from = edge['from']['extracted_elem']
            extracted_to = edge['to']['extracted_elem']
            for felem in extracted_from:
                for telem in extracted_to:
#                     print('{} - src: {} - dest: {} - attr: {}'.format(count, src_type_name, dst_type_name, e))
                    from_id = construct_key(edge['from'], felem, add_type_to_key, '_UNKNOWN_')
                    to_id = construct_key(edge['to'], telem, add_type_to_key, '_UNKNOWN_')

                    attr = dict()
                    attr['_type_'] = edge['type']
                    for k,v in edge['from']['attr_info'].items():
                        attr[k] = felem[v] if v in felem else ''
                    for k,v in edge['to']['attr_info'].items():
                        attr[k] = telem[v] if v in telem else ''
#                     print('adding edge from: {} -> to: {}, attr: {}'.format(from_id, to_id, attr))
                    graph.add_edge(from_id, to_id, **attr)
                    count += 1                
    # if verbose:
    #     print('type: {} -> {} - {}'.format(src_type_path, dst_type_path, count))
    
    return graph
            


def create_graph_clique_from_json(graph, graph_mapper, data_provider, add_type_to_key, update=True, verbose=False):

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
        nodes = clique['nodes']
        node_list = []

        # collect node information needed to pick data.
        for node_info in nodes:
            # TBD... assert check_attributes(node_info, raw_data, node_info['attributes'])        
        #         print('node_info to process:', node_info)
            # node_info['path'] = correct_path(node_info['path'])
            # for key in node_info['key']:
            #     key['raw'] = correct_path(key['raw'])
            node_info = configure_node_info(node_info)

            attr_dict = {}
            if 'attributes' in node_info:
                for a in node_info['attributes']:
                    raw_attr = a['raw']
                    raw_attr = correct_path(raw_attr)
                    pattern = re.compile(a['pattern']) if 'pattern' in a else None
                    attr_dict[a['name']] = (raw_attr, pattern)
            node_info['attr_info'] = attr_dict 
            
            # construct attribute mapping between raw_attrib_name -> attrib_name
            lookup_attr_list = list(attr_dict.values())
        
            # for k, v in attr_dict.items():
            #     v = correct_path(v)
            #     lookup_attr_list.append(v)

            # for key in node_info['key']:
            #     if key['raw'] not in lookup_attr_list:
            #         lookup_attr_list.append(key['raw'])

            lookup_attr_list = append_keys_to_lookup_attributes(node_info, lookup_attr_list)
            node_info['lookup_attr_list'] = lookup_attr_list            

            # print('node_meta:', node_meta)
            node_list.append(node_info)
            
        attr = dict()
        count = 0

        # iterate and collect.  
        for j in raw_data:
#             print('json>> ', j)
            for node in node_list:
                jelem = extract_node_attrs_from_json(j, node)
                node['extracted_elem'] = jelem

            # construct the clique
            for src_node in node_list:
                for src_elem in src_node['extracted_elem']:
                    # print('{} - type_found: {} - attr: {}'.format(count, node_type_name, e))
                    src_node_id = construct_key(src_node, src_elem, add_type_to_key)

                    for dst_node in node_list:
                        if src_node == dst_node: # we don't allow same type to link (TBD: we need to reconsider this!)
                            continue
                        for dst_elem in dst_node['extracted_elem']:
                            dst_node_id = construct_key(dst_node, dst_elem, add_type_to_key)

                            attr['_type_'] = clique_type_name
                            # print('adding edge from: {} -> to: {}, attr: {}'.format(src_node_id, dst_node_id, attr))
                            graph.add_edge(src_node_id, dst_node_id, **attr)

                            count += 1
        
                            # print('type: {} - {}'.format(node_type_path, count))
            if verbose:
                print('num cliques creates: {}'.format(count))

        
    return graph

   
