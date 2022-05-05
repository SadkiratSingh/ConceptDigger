import os
import json
import socket
import time
import types
from owlready2 import *

from GraphCreator import GraphCreator
from utilities.constants import category_word_language_mapping




class GraphTraversor(GraphCreator):
    def __init__(self, create_fresh:bool, datafolderprefix:str, language_code:str):
        super().__init__(create_fresh, datafolderprefix, language_code)
        
        #service will start at localhost with the specied port
        self._address = "" 
        self._port = 5009
        
        onto_path.append("/home/sadkiratsinghubuntu/wikipedia-digger/ConceptDigger/{datafolderprefix}")
        
        #start the service and process the input
        self._sok = socket.socket()        
        self._start_service()
        self._process_input()
    
    @staticmethod
    def cleanText(entity):
        symbols_list = ["'", '"', "(", ")"]
        for s in symbols_list:
            entity = entity.replace(s, "")
        return entity
    
    @staticmethod
    def defineAnnotationProperties(onto):
        with onto:
            class has_synonym(AnnotationProperty):pass
        with onto:
            class has_uri(AnnotationProperty):pass
    
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
                    onto_child_class = types.new_class(refined_child_uri, tuple([onto_parent_class]))
                    print(f"child category---> {refined_child_uri}")
                    print(f"parent category--->{onto_parent_class}")
                    onto[onto_child_class.name].label.append(child_uri)
                    onto[onto_child_class.name].has_uri.append(f"https://{self._language_code}.wikipedia.org/wiki/{child_uri}")
                    
                    if child_uri[0:len(category_word)+1] == f'{category_word}:':
                        
                        if curDepth < maxDepth: 
                            self._prepare_ontology(child,onto_child_class,curDepth+1,maxDepth,onto)
                            
                    else:
                        synonyms_list = list(synonyms_map[graph.vertex(child)])
                        onto[onto_child_class.name].has_synonym = synonyms_list
            
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
                ontology_name = subcategory[1]
                maxDepth =  subcategory[2]
                print ('seedCategory = ' + seedCategory)
                print ('maxDepth = ' + str(maxDepth))

                seedCategoryId = self._nodesdict[seedCategory]
                rel_ontology_filename = f"{ontology_name}.owl"
                abs_ontology_filename = f"/home/sadkiratsinghubuntu/wikipedia-digger/ConceptDigger/{self._datafolderprefix}{rel_ontology_filename}"
                if(os.path.isfile(abs_ontology_filename)):
                    os.remove(abs_ontology_filename)
                    
                #initialize empty ontology
                onto = get_ontology(abs_ontology_filename)
                
                #define annotation properties for the ontology
                GraphTraversor.defineAnnotationProperties(onto)
                
                with onto:
                    refined_seed_category = GraphTraversor.cleanText(seedCategory)
                    onto_parent_class = types.new_class((refined_seed_category),tuple([Thing]))
                    onto[onto_parent_class.name].label.append(seedCategory)
                    onto[onto_parent_class.name].has_uri.append(f"https://{self._language_code}.wikipedia.org/wiki/{seedCategory}")
                self._prepare_ontology(seedCategoryId,onto_parent_class,0,maxDepth,onto)
                onto.save(abs_ontology_filename)

                t1 = time.time()      
                conn.send(f"ontology perpared for {seedCategory} in {str(t1-t0)} seconds".encode())

            conn.close()


