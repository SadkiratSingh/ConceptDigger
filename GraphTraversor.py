import os
import json
import socket
import time
import types
import re
from owlready2 import *

from GraphCreator import GraphCreator
from utilities.constants import category_word_language_mapping
from OntologyManager import Ontology_Manager




class GraphTraversor(GraphCreator):
    def __init__(self, create_fresh:bool, datafolderprefix:str, language_code:str):
        super().__init__(create_fresh, datafolderprefix, language_code)
        
        #service will start at localhost with the specied port
        self._address = "" 
        self._port = 5009
        
        #start the service and process the input
        self._sok = socket.socket()        
        self._start_service()
        self._process_input()
    
    @staticmethod
    def cleanText(entity):
        pattern = re.compile(r"[^A-Za-z\.0-9\_\-\:]")
        symbols_to_escape = pattern.findall(entity)
        for s in symbols_to_escape:
            entity = entity.replace(s, "_")
        entity = entity.strip("_")
        return entity
    
    def _prepare_ontology(self,parentId,onto_parent_class,curDepth,maxDepth,onto):
        with onto:
            graph = self._graph
            name_map = self._maps["name_map"]
            synonyms_map = self._maps["synonyms_map"]
            category_word = category_word_language_mapping[self._language_code]
            
            #the current category node we are processing becomes a parent node
            parentNode = graph.vertex(parentId)
            
            #get all the children of the current category node(which becomes parent node as above) we are processing
            successors = graph.iter_out_neighbors(parentNode)

            for child in successors:
                child_uri = name_map[graph.vertex(child)]
                refined_child_uri = GraphTraversor.cleanText(child_uri)
                child_onto_ancestors = [c.name for c in list(onto[onto_parent_class.name].ancestors())]
                
                if refined_child_uri not in child_onto_ancestors:
                    print(f"child class---> {refined_child_uri}")
                    print(f"parent class--->{onto_parent_class}")
                                        
                    if child_uri[0:len(category_word)+1] == f'{category_word}:':
                        child_class=Ontology_Manager.add_class_pair(refined_child_uri, onto_parent_class)
                        child_class.label.append(child_uri)
                        child_class.has_uri.append(f"https://{self._language_code}.wikipedia.org/wiki/{child_uri}")
                        if curDepth < maxDepth: 
                            self._prepare_ontology(child,child_class,curDepth+1,maxDepth,onto)
                            
                    else:
                        child_individual = onto_parent_class(refined_child_uri)
                        child_individual.label.append(child_uri)
                        child_individual.has_uri.append(f"https://{self._language_code}.wikipedia.org/wiki/{child_uri}")
                        synonyms_list = list(synonyms_map[graph.vertex(child)])
                        for synonym_detail_string in synonyms_list:
                            #TODO: than just simply adding synonyms here, we will add some annotations to each synonym as well.
                            synonym_details = json.loads(synonym_detail_string)
                            synonym_value = synonym_details["value"]
                            synonym_source = synonym_details["source"]
                            child_individual.has_synonym.append(synonym_value)
                            onto.has_synonym_source[child_individual, onto.has_synonym,synonym_value] = [synonym_source]
                            
            
    def _start_service(self):
        self._sok.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sok.bind((self._address, self._port))
        self._sok.listen(5)
        print ('DBPedia Category & Page Digger & Synonym finder is listening on ' + self._address + ':' + str(self._port), '\nPress Ctrl+C to stop the server.')
    
    def _process_input(self):
        while True:
            conn, addr = self._sok.accept()
            packet = conn.recv(10240)
            input = json.loads( packet )
            
            for subcategory in input:
                t0 = time.time() 
            
                seedCategory = subcategory[0]
                maxDepth =  subcategory[1]
                ontology_name = subcategory[2]
                create_new_ontology = subcategory[3]
                delete_existing_ontology = subcategory[4]
            
                Ontology_Manager.set_ontology_name(ontology_name)
                if(bool(delete_existing_ontology)):
                    Ontology_Manager.delete_ontology()
                else:
                    Ontology_Manager.load_ontology(bool(int(create_new_ontology)))
                    onto = Ontology_Manager.ontology
                    print ('seedCategory = ' + seedCategory)
                    print ('maxDepth = ' + str(maxDepth))
                    seedCategoryId = self._nodesdict[seedCategory]             
                    refined_seed_category = GraphTraversor.cleanText(seedCategory)
                    
                    # TODO: check if this seed class already exists in ontology
                    seed_class = None
                    seed_class_exists = onto.search(iri=f"{onto.base_iri}{refined_seed_category}")
                    if(len(seed_class_exists) == 0):
                        seed_class = Ontology_Manager.add_class_pair(refined_seed_category)
                        seed_class.label.append(seedCategory)
                        seed_class.has_uri.append(f"https://{self._language_code}.wikipedia.org/wiki/{seedCategory}")
                    else:
                        seed_class = seed_class_exists[0]
                    self._prepare_ontology(seedCategoryId,seed_class,0,maxDepth,onto)
                    
                    Ontology_Manager.saving_ontology_to_files()
                    Ontology_Manager.commit_changes_to_quadstore()

                    t1 = time.time()      
                    conn.send(f"{seedCategory} ontology appended in {str(t1-t0)} seconds".encode())

            conn.close()


