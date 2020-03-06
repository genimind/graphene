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

import re
'''
Utility functions
'''
def correct_path(str_val):
    if str_val[-1] != '/':
        str_val += '/' 
    return str_val

def construct_key(node_info, node_data, add_type_to_key, invalid_value = None):
    # print('in.... get_key_list() - node_info:', node_info)
    key_list = []
    if add_type_to_key:
        key_list.append(node_info['type'])

    for key in node_info['key_info']:
        key_value = node_data[key] if key in node_data else invalid_value
        key_list.append(str(key_value))

    return tuple(key_list)


def valid_path_to_pick(cur_path, type_path, is_relative):
    # type_path can have two styles, absolute or partial
    # absolute: /a/b/c/ 
    # relative: .../b/c/
    if is_relative:
        # compare from right
        # t_path = type_path.replace('...', '')
        return cur_path.endswith(type_path)
    else:
        return cur_path == type_path

def attr_in_attrlist(cur_path, attr_list, is_relative = False):
    '''
    return the correct attribute name from the mapper model if exist in the path
    we are evaluating, to allow for support of relative path and futher processing
    when we construct the node attributes.
    '''
    ret_attr = None
    if is_relative:
        # check each attribute from the end of the current path
        for attr in attr_list:
            # attr_temp = attr.replace('...','')
            # attr is a tuple of (raw, pattern)
            if cur_path.endswith(attr[0]):
                ret_attr = attr
                break
    else:
        for attr in attr_list:
            if cur_path == attr[0]:
                ret_attr= attr
                break

    return ret_attr


def configure_node_info(node_info):
    node_info['is_relative'] = False
    node_info['is_list'] = False

    if node_info['path'].startswith('...'): # relative_path
        node_info['is_relative'] = True
        node_info['path'] = node_info['path'].replace('...','')

    if node_info['path'].endswith('[]'): # nodes from list
        node_info['is_list'] = True
        node_info['path'] = node_info['path'].replace('[]','')

    node_info['path'] = correct_path(node_info['path'])

    key_items = []
    for key in node_info['key']:
        key['raw'] = correct_path(key['raw'])
        pattern = re.compile(key['pattern']) if 'pattern' in key else None
        key_info = (key['raw'], pattern)
        key_items.append(key_info)
    node_info['key_info'] = key_items
    
    return node_info


def append_keys_to_lookup_attributes(node_info, attr_list):
    for key in node_info['key_info']:
        if key not in attr_list:
            attr_list.append(key)

    return attr_list
        
