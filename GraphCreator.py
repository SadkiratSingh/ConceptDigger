import os
import pickle
import re
import time
from os.path import expanduser

from graph_tool.all import *


class GraphCreator:
    def __init__(self, create_fresh: bool, datafolderprefix: str, language_code: str):
        graph_loc = expanduser("~") + "/wikipedia-digger" + "/ConceptDigger"
        os.chdir(graph_loc)
        self._graph = Graph()
        self._nodesdict = {}
        self._datafolderprefix = datafolderprefix
        self._create_fresh = create_fresh
        self._language_code = language_code
        if(self._create_fresh == True):
            self.__construct_graph_from_scratch()
        else:
            self.__load_graph_from_disk()
    
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
        t0 = time.time()
        print ("processing " + filename + " ...")
        f = open(filename, 'r')
        
        for line in f.readlines():
            if line.find(predicate_keyword)>0:
                #extract the subject - which is actually the child resource(article or subcategory) node
                match_obj = re.search("^<http://.{2}\.", line)
                if match_obj is None:
                    subject = line[29:line.find(">",29)]
                else:
                    subject = line[32:line.find(">",32)]
                #extract the object - which is actually the parent resource(category) node
                obj = line[line.rfind("resource/")+9:line.rfind(">")]
                
                #if node is not in the graph, then create a new node for it
                if subject not in self._nodesdict :
                    node = self._graph.add_vertex()
                    self._nodesdict[subject] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = subject
                    self._maps["synonyms_map"][node].append(self.__clean_text(subject))
                    
                if obj not in self._nodesdict :
                    node = self._graph.add_vertex()
                    self._nodesdict[obj] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = obj
                    self._maps["synonyms_map"][node].append(self.__clean_text(obj))

                #add an edge to depict the child-parent relation
                childNode = self._nodesdict[subject]
                parentNode = self._nodesdict[obj]              
                self._graph.add_edge(self._graph.vertex(parentNode),self._graph.vertex(childNode))
                
        f.close()
        t1 = time.time()
        print ("processed " + filename + " in " + str(t1-t0) + " seconds") 
    
    def __assignSynonymFromURI(self, filename, predicate_keyword):
        t0 = time.time()
        print ("processing " + filename + " ...")
        f = open(filename, 'r')

        for line in f.readlines():
            if line.find(predicate_keyword)>0:
                #extract the subject which will be treated as synonym node
                match_obj = re.search("^<http://.{2}\.", line)
                if match_obj is None:
                    subject = line[29:line.find(">",29)]
                else:
                    subject = line[32:line.find(">",32)]
                #extract the object which will be treated as resource(article) node
                obj = line[line.rfind("resource/")+9:line.rfind(">")]

                
                if subject not in self._nodesdict:
                    node = self._graph.add_vertex()
                    self._nodesdict[subject] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = subject
                    self._maps["synonyms_map"][node].append(self.__clean_text(subject))

                if obj not in self._nodesdict :
                    node = self._graph.add_vertex()
                    self._nodesdict[obj] = self._graph.vertex_index[node]
                    self._maps["name_map"][node] = obj
                    self._maps["synonyms_map"][node].append(self.__clean_text(obj))
                
                refined_subject = self.__clean_text(subject)
                
                if refined_subject not in self._maps["synonyms_map"][self._graph.vertex(self._nodesdict[obj])]:
                    self._maps["synonyms_map"][self._graph.vertex(self._nodesdict[obj])].append(refined_subject)

        f.close()
        t1 = time.time()
        print ("processed " + filename + " in " + str(t1-t0) + " seconds") 

    def __assignSynonymFromLiteral(self, filename, predicate_keywords):
        t0 = time.time()
        print ("processing " + filename + " ...")
        f = open(filename, 'r')
        
        for line in f.readlines():
            for keyword in predicate_keywords:
                if line.find(keyword)>0:
                    
                    #extract the subject which will be treated as resource(article) node
                    match_obj = re.search("^<http://.{2}\.", line)
                    if match_obj is None:
                        subject = line[29:line.find(">",29)]
                    else:
                        subject = line[32:line.find(">",32)]
                        
                    #extract the object which will be a literal
                    searchObj = re.search('".*"', line) #searches for literal enclosed between "double quotes"
                    obj = searchObj.group()[1:-1] #removes the trailing and beginning double quote character from literal
                    obj = obj.lower()
                    
                    if subject not in self._nodesdict:
                        node = self._graph.add_vertex()
                        self._nodesdict[subject] = self._graph.vertex_index[node]
                        self._maps["name_map"][node] = subject
                        self._maps["synonyms_map"][node].append(self.__clean_text(subject))
                        
                    if obj not in self._maps["synonyms_map"][self._graph.vertex(self._nodesdict[subject])]:
                        self._maps["synonyms_map"][self._graph.vertex(self._nodesdict[subject])].append(obj)
        f.close()
        t1 = time.time()
        print ("processed " + filename + " in " + str(t1-t0) + " seconds") 
    
    def __clean_text(self, text):
        return text.replace("(disambiguation)","")
    
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
    
    def __construct_graph_from_scratch(self):
        self.__assign_property_maps()
        self._maps["language_code"][self._graph] = self._language_code
        t0 = time.time()
        self.__insert_nodes(self._datafolderprefix + f"categories_lang={self._language_code}_skos.ttl", "/skos/core#broader")
        self.__insert_nodes(self._datafolderprefix + f"categories_lang={self._language_code}_articles.ttl", "purl.org/dc/terms/subject")
        self.__assignSynonymFromURI(self._datafolderprefix + f"redirects_lang={self._language_code}.ttl", 'http://dbpedia.org/ontology/wikiPageRedirects')
        self.__assignSynonymFromURI(self._datafolderprefix + f"disambiguations_lang={self._language_code}.ttl", "http://dbpedia.org/ontology/wikiPageDisambiguates")
        self.__assignSynonymFromLiteral(self._datafolderprefix + f"mappingbased-literals_lang={self._language_code}.ttl" , ["http://dbpedia.org/ontology/alias", "http://xmlns.com/foaf/0.1/name"])
        t1 = time.time()
        print ("Graph loaded in : " + str(t1-t0) + " seconds")
        self.__save_graph_to_disk()

        return self._graph, self._maps, self._language_code, self._nodesdict

    def __save_graph_to_disk(self):
        self.__internalize_property_maps()
        print ("dumping pickle files")
        self._graph.save(self._datafolderprefix + "wiki_graph.gt")
        pickle.dump(self._nodesdict, open(self._datafolderprefix + "nodesDict.pickle","wb"), protocol=2)

    def __load_graph_from_disk(self):
        print ('Loading CACHE of graph data from hardisk..')
        t0 = time.time()
        self._graph = load_graph(self._datafolderprefix + "wiki_graph.gt")
        self._nodesdict = pickle.load(open(self._datafolderprefix + "nodesDict.pickle","rb"))
        self.__assign_property_maps()
        print(self._maps["language_code"])
        self._language_code = self._maps["language_code"]
        t1 = time.time()
        print ("Time to load CACHE of graph data (from pickle files) = " + str(t1-t0))

        return self._graph, self._maps, self._language_code, self._nodesdict
    