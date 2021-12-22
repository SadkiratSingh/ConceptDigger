import os
import pickle
import re
import time
from os.path import expanduser

from graph_tool.all import *


class GraphHandler:
    def __init__(self, create_fresh: bool, datafolderprefix: str, language_code: str):
        graph_loc = expanduser("~") + "/train-lexical-analyzer" + "/ConceptDigger"
        os.chdir(graph_loc)
        self._graph = Graph()
        self._nodesdict = {}
        self._datafolderprefix = datafolderprefix
        self._create_fresh = create_fresh
        self._language_code = language_code
        if(self._create_fresh == True):
            self._construct_graph_from_scratch()
        else:
            self._load_graph_from_disk()
    
    @property
    def _graph(self):
        return self.__graph
    @_graph.setter
    def _graph(self, val):
        self.__graph = val

    @property
    def _datafolderprefix(self):
        return self.__datafolderprefix
    @_datafolderprefix.setter
    def _datafolderprefix(self, val):
        self.__datafolderprefix = val

    @property
    def _create_fresh(self):
        return self.__create_fresh
    @_create_fresh.setter
    def _create_fresh(self, val):
        self.__create_fresh = val
    
    @property
    def _nodesdict(self):
        return self.__nodesdict
    @_nodesdict.setter
    def _nodesdict(self, val):
        self.__nodesdict = val
    
    @property
    def _language_code(self):
        return self.__language_code
    @_language_code.setter
    def _language_code(self, val):
        self.__language_code = val
    
    @property
    def _maps(self):
        return self.__maps
    @_maps.setter
    def _maps(self, val):
        self.__maps = val

    def __insert_nodes(self, filename, predicate_keyword):
        t2 = time.time()
        print ("processing " + filename + " ...")
        f = open(filename, 'r')
        for line in f.readlines():
            if line.find(predicate_keyword)>0:
                #extract the subject - which is actually the child node
                match_obj = re.search("^<http://.{2}\.", line)
                if match_obj is None:
                    subject = line[29:line.find(">",29)]
                else:
                    subject = line[32:line.find(">",32)]
                #extract the object - which is actually the parent node
                obj = line[line.rfind("resource/")+9:line.rfind(">")]
                
                #if node is note already added in graph, then create a new node for it
                if subject not in self._nodesdict :
                    node = self._graph.add_vertex()
                    self._nodesdict[subject] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = subject
                    self._maps["synonyms_map"][node].append(self.__convert_entity_URI_to_label(subject))
                if obj not in self._nodesdict :
                    node = self._graph.add_vertex()
                    self._nodesdict[obj] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = obj
                    self._maps["synonyms_map"][node].append(self.__convert_entity_URI_to_label(obj))

                #add an edge to depict the child-parent relation
                childNode = self._nodesdict[subject]
                parentNode = self._nodesdict[obj]              
                self._graph.add_edge(self._graph.vertex(parentNode),self._graph.vertex(childNode))
        f.close()
        t3 = time.time()
        print ("processed " + filename + " in " + str(t3-t2) + " seconds") 
    
    def __assignSynonymFromURI(self, filename, predicate_keywords):
        t2 = time.time()
        print ("processing " + filename + " ...")
        f = open(filename, 'r')

        for line in f.readlines():
            for keyword in predicate_keywords:
                if line.find(keyword)>0:
                    #extract the subject
                    match_obj = re.search("^<http://.{2}\.", line)
                    if match_obj is None:
                        subject = line[29:line.find(">",29)]
                    else:
                        subject = line[32:line.find(">",32)]
                    #extract the object
                    obj = line[line.rfind("resource/")+9:line.rfind(">")]

                    #extract label of URI
                    if keyword != "http://dbpedia.org/ontology/wikiPageRedirects":
                        if subject not in self._nodesdict:
                            node = self._graph.add_vertex()
                            node_index = self._graph.vertex_index[node]
                            self._nodesdict[subject] = node_index
                            self._maps["name_map"][node] = subject
                            label = self.__convert_entity_URI_to_label(obj)
                            self._maps["synonyms_map"][node].append(label)

                    if obj not in self._nodesdict :
                        node = self._graph.add_vertex()
                        node_index = self._graph.vertex_index[node]
                        self._nodesdict[obj] = node_index
                        self._maps["name_map"][node] = obj
                        label = self.__convert_entity_URI_to_label(obj)
                        self._maps["synonyms_map"][node].append(label)

                    if keyword == "http://dbpedia.org/ontology/wikiPageRedirects":
                        object_node = self._graph.vertex(self._nodesdict[obj])
                        if [e for e in self._maps["synonyms_map"][object_node] if e == subject] == []:
                            self._maps["synonyms_map"][object_node].append(subject)
                    elif keyword == "http://dbpedia.org/ontology/type":
                        subject_node = self._graph.vertex(self._nodesdict[subject])
                        self._maps["type_map"][subject_node] = obj
                    elif keyword == "http://dbpedia.org/ontology/hasVariant":
                        subject_node = self._graph.vertex(self._nodesdict[subject])
                        if [e for e in self._maps["variants_map"][subject_node] if e == obj] == []:
                            self._maps["variants_map"][subject_node].append(obj)
                    elif keyword == "http://www.w3.org/2000/01/rdf-schema#seeAlso":
                        subject_node = self._graph.vertex(self._nodesdict[subject])
                        if [e for e in self._maps["references_map"][subject_node] if e == obj] == []:
                            self._maps["references_map"][subject_node].append(obj)

        f.close()
        t3 = time.time()
        print ("processed " + filename + " in " + str(t3-t2) + " seconds") 

    
    def __convert_entity_URI_to_label(self, entity_URI):
        return entity_URI.replace("_"," ")
    
    def __assign_property_maps(self):
        self._maps = {
            "name_map": self._graph.new_vertex_property("string") if self._create_fresh == True else self._graph.vertex_properties["name_map"],
            "synonyms_map": self._graph.new_vertex_property("vector<string>") if self._create_fresh == True else self._graph.vertex_properties["synonyms_map"],
            "variants_map": self._graph.new_vertex_property("vector<string>") if self._create_fresh == True else self._graph.vertex_properties["variants_map"],
            "references_map": self._graph.new_vertex_property("vector<string>") if self._create_fresh == True else self._graph.vertex_properties["references_map"],
            "type_map": self._graph.new_vertex_property("string") if self._create_fresh == True else self._graph.vertex_properties["type_map"],
            "language_code": self._graph.new_graph_property("string") if self._create_fresh == True else self._graph.graph_properties["language_code"]
        }
    
    def __internalize_property_maps(self):
        for key in self._maps.keys():
            if key.find("map") == -1:
                self._graph.graph_properties[key] = self._maps[key]
            else:
                self._graph.vertex_properties[key] = self._maps[key]
    
    def _construct_graph_from_scratch(self):
        self.__assign_property_maps()
        self._maps["language_code"][self._graph] = self._language_code
        t0 = time.time()
        self.__insert_nodes(self._datafolderprefix + f"categories_lang={self._language_code}_skos.ttl", "/skos/core#broader")
        self.__insert_nodes(self._datafolderprefix + f"categories_lang={self._language_code}_articles.ttl", "purl.org/dc/terms/subject")
        self.__assignSynonymFromURI(self._datafolderprefix + f"redirects_lang={self._language_code}.ttl", ['http://dbpedia.org/ontology/wikiPageRedirects'])
        self.__assignSynonymFromURI(self._datafolderprefix + f"mappingbased-objects_lang={self._language_code}.ttl" , ['http://dbpedia.org/ontology/type','http://dbpedia.org/ontology/hasVariant','http://www.w3.org/2000/01/rdf-schema#seeAlso'])
        t1 = time.time()
        print ("Graph loaded in : " + str(t1-t0) + " seconds")
        self.__save_graph_to_disk()

    def __save_graph_to_disk(self):
        self.__internalize_property_maps()
        print ("dumping pickle files")
        self._graph.save(self._datafolderprefix + "wiki_graph.gt")
        pickle.dump(self._nodesdict, open(self._datafolderprefix + "nodesDict.pickle","wb"), protocol=2)

    def _load_graph_from_disk(self):
        print ('Loading CACHE of graph data from hardisk..')
        t0 = time.time()
        self._graph = load_graph(self._datafolderprefix + "wiki_graph.gt")
        self._nodesdict = pickle.load(open(self._datafolderprefix + "nodesDict.pickle","rb"))
        self.__assign_property_maps()
        print(self._maps["language_code"])
        self._language_code = self._maps["language_code"]
        t1 = time.time()
        print ("Time to load CACHE of graph data (from pickle files) = " + str(t1-t0))
