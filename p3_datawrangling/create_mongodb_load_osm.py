
#!/usr/bin/env python
# -*- coding: utf-8 -*-
##I used the code for creating a sample from Udacity to create the full JSON data set.

import xml.etree.cElementTree as ET  # Use cElementTree or lxml if too slow
import pprint
import re
import codecs
import json

OSM_FILE = "../phoenix_arizona.osm"  # Replace this with your osm file
SAMPLE_FILE = "../phoenix_arizona.osm"

k = 10 # Parameter: take every k-th top level element

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


    with open(SAMPLE_FILE, 'wb') as output:
        output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        output.write('<osm>\n  ')
    
        # Write every kth top level element
        for i, element in enumerate(get_element(OSM_FILE)):
            output.write(ET.tostring(element, encoding='utf-8'))
    
        output.write('</osm>')
        
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
problematic_cities = {'MEsa': 'Mesa','tempe' : 'Tempe'
                      ,'Paradise Valley, AZ' : 'Paradise Valley'
                      , 'chandler' : 'Chandler','CHANDLER' : 'Chandler'
                      ,'Mesa, AZ' :  'Mesa','tEMPE' : 'Tempe'
                      ,'casa Grande' : 'Casa Grande'
                      ,'sun City West' : 'Sun City West'
                      ,'peoria' : 'Peoria'
                      ,'SanTan Valley' : 'San Tan Valley'
                      ,'scottsdale' : 'Scottsdale','mesa': 'Mesa'}

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :

        created = {}
        for e in element.attrib.keys():
            
            if e in CREATED:
                created[e] = element.attrib[e]
            elif element.attrib[e] == element.get('lat') or element.attrib[e] == element.get('lon'):
                pos = []
                pos.append(float(element.get('lat')))
                pos.append(float(element.get('lon')))
                node['pos'] = pos
            else:
                node[e] = element.get(e)
                node['type'] = element.tag
        node['created'] = created
        
        node_refs = []
        address = {}
        for subtag in element:
            if subtag.tag == 'tag':
                if re.search(problemchars, subtag.get('k')):
                    pass
                elif re.search(r'\w+:\w+:\w+', subtag.get('k')):
                    pass
                elif subtag.get('k').startswith('addr'):
                    if subtag.get('v') in problematic_cities:
                        address[subtag.get('k')[5:]] = problematic_cities[subtag.get('v')]
                        node['address'] = address
                    else:
                        address[subtag.get('k')[5:]] = subtag.get('v')
                        node['address'] = address
                else:
                    node[subtag.get('k')] = subtag.get('v')
            else:
                if subtag.tag == 'nd':
                    node_refs.append(subtag.get('ref'))
                else:
                    pass
        
        if node_refs:
            
            node['node_refs'] = node_refs
                
        
        return node
    else:
        return None
        

def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

if __name__ == "__main__":
    get_element(OSM_FILE)
    process_map(SAMPLE_FILE)
    