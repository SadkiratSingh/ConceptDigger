import copy
import json
import socket
import time

from GraphCreator import GraphCreator
from mongonlx.dbaccess.writetodb import insert_one_concept
from utilities.constants import category_word_language_mapping


class GraphTraversor(GraphCreator):
    def __init__(self, create_fresh:bool, datafolderprefix:str, language_code:str):
        super().__init__(create_fresh, datafolderprefix, language_code)
        self._enumerated_child_in_list = []
        self._node_names_set = set()
        self._address = "" 
        self._port = 5008
        self._sok = socket.socket()
        self._start_service()
        self._process_input()
    
    @property
    def _enumerated_child_in_list(self):
        return self.__enumerated_child_in_list
    @_enumerated_child_in_list.setter
    def _enumerated_child_in_list(self,val):
        self.__enumerated_child_in_list = val

    @property
    def _node_names_set(self):
        return self.__node_names_set
    @_node_names_set.setter
    def _node_names_set(self, val):
        self.__node_names_set = val

    def _clearDataStructuresUsedInProcessing(self):
        self._enumerated_child_in_list.clear()
        self._node_names_set.clear()

    def _enumerateChild(self,fromParent,curDepth,maxDepth,returnCategories,returnPages,seedCategoryParent,ancestors):
        graph = self._graph
        lang_code = self._language_code
        name_map = self._maps["name_map"]
        category_word = category_word_language_mapping[lang_code]
        parentNode = graph.vertex(fromParent)
        parentName = name_map[parentNode]
        successors = graph.iter_out_neighbors(parentNode)
        ancestors.append(parentName)
        a_list = copy.deepcopy(ancestors)


        for child in successors:
            childName = name_map[graph.vertex(child)]
            if childName[0:len(category_word)+1] == f'{category_word}:':
                if curDepth < maxDepth: self._enumerateChild(child,curDepth+1,maxDepth,returnCategories,returnPages,seedCategoryParent,ancestors)
                if returnCategories==1:
                    if childName not in self._node_names_set:
                        childNodeData = {}
                        childNodeData["index"] = child
                        childNodeData["name"] = childName
                        childNodeData["level"] = curDepth
                        childNodeData["ancestors"] = a_list
                        self._enumerated_child_in_list.append(childNodeData)
                        self._node_names_set.add(childName)
            else:
                if returnPages==1:
                    if childName not in self._node_names_set:
                        childNodeData = {}
                        childNodeData["index"] = child
                        childNodeData["name"] = childName
                        childNodeData["level"] = curDepth
                        childNodeData["ancestors"] = a_list
                        self._enumerated_child_in_list.append(childNodeData)
                        self._node_names_set.add(childName)
        ancestors.pop()

    def _prepareOutput(self, col_name):
        graph = self._graph
        synonyms_map = self._maps["synonyms_map"]
        references_map = self._maps["references_map"]
        type_map = self._maps["type_map"]
        variants_map = self._maps["variants_map"]
        lang_code = self._language_code

        for child in self._enumerated_child_in_list:
            entity_name = child["name"]
            level = child["level"]
            ancestors = child["ancestors"]
            start_category = ancestors[0]
            end_category = ancestors[len(ancestors)-1]
            synonyms = list(synonyms_map[graph.vertex(child["index"])])
            references = list(references_map[graph.vertex(child["index"])])
            variants = list(variants_map[graph.vertex(child["index"])])
            types = type_map[graph.vertex(child["index"])]
            surface_text = synonyms[0]
            concept_doc = {
                "entity_url":f"http://{lang_code}.wikipedia.org/wiki/{entity_name}",
                "surface_text": surface_text,
                "synonyms": synonyms,
                "level": level,
                "start_category": start_category,
                "end_category": end_category,
                "references": references,
                "variants": variants,
                "types": types
            }
            insert_one_concept(concept_doc, col_name, lang_code)


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
                colName = subcategory[1]
                maxDepth =  subcategory[2]
                returnCategories = subcategory[3]
                returnPages = subcategory[4]
                print ('seedCategory = ' + seedCategory)
                print ('maxDepth = ' + str(maxDepth))

                seedCategoryNode = self._nodesdict[seedCategory]
                self._enumerateChild(seedCategoryNode,0,maxDepth,returnCategories,returnPages, seedCategoryNode,[])
                self._prepareOutput(colName)
                self._clearDataStructuresUsedInProcessing() 

                t1 = time.time()      
                conn.send(f"{seedCategory} processed in {str(t1-t0)} seconds".encode())

            conn.close()

