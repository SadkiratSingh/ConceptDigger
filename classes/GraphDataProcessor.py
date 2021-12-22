import copy

from mongonlx.dbaccess.writetodb import insert_one_concept
from utilities.constants import category_word_language_mapping

from classes.GraphHandler import GraphHandler


class GraphDataProcessor(GraphHandler):
    def __init__(self, create_fresh:bool, datafolderprefix:str, language_code:str):
        super().__init__(create_fresh, datafolderprefix, language_code)
        self._maxDepth = 4
        self._enumeratedChildInList = []
        self._nodeNamesSet = set()
        self._mongoColname = ""
    
    @property
    def _maxDepth(self):
        return self.__maxDepth
    @_maxDepth.setter
    def _maxDepth(self, val):
        self.__maxDepth = val
    
    @property
    def _mongoColname(self):
        return self.__mongoColname
    @_mongoColname.setter
    def _mongoColname(self, val):
        self.__mongoColname = val

    @property
    def _enumeratedChildInList(self):
        return self.__enumeratedChildInList
    @_enumeratedChildInList.setter
    def _enumeratedChildInList(self,val):
        self.__enumeratedChildInList = val

    @property
    def _nodeNamesSet(self):
        return self.__nodeNamesSet
    @_nodeNamesSet.setter
    def _nodeNamesSet(self, val):
        self.__nodeNamesSet = val

    def _clearDataStructuresUsedInProcessing(self):
        self._enumeratedChildInList.clear()
        self._nodeNamesSet.clear()

    def _enumerateChild(self,fromParent,curDepth,returnCategories,returnPages,seedCategoryParent,ancestors):
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
                if curDepth < self._maxDepth: self._enumerateChild(child,curDepth+1,returnCategories,returnPages,seedCategoryParent,ancestors)
                if returnCategories==1:
                    if childName not in self._nodeNamesSet:
                        childNodeData = {}
                        childNodeData["index"] = child
                        childNodeData["name"] = childName
                        childNodeData["level"] = curDepth
                        childNodeData["ancestors"] = a_list
                        self._enumeratedChildInList.append(childNodeData)
                        self._nodeNamesSet.add(childName)
            else:
                if returnPages==1:
                    if childName not in self._nodeNamesSet:
                        childNodeData = {}
                        childNodeData["index"] = child
                        childNodeData["name"] = childName
                        childNodeData["level"] = curDepth
                        childNodeData["ancestors"] = a_list
                        self._enumeratedChildInList.append(childNodeData)
                        self._nodeNamesSet.add(childName)
        ancestors.pop()

    def _prepareOutput(self):
        output = []
        graph = self._graph
        synonyms_map = self._maps["synonyms_map"]
        references_map = self._maps["references_map"]
        type_map = self._maps["type_map"]
        variants_map = self._maps["variants_map"]
        lang_code = self._language_code

        for child in self._enumeratedChildInList:
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
            insert_one_concept(concept_doc, self._mongoColname, lang_code)
            out_str = f"entity_url : http://{lang_code}.wikipedia.org/wiki/{entity_name}, surface_text : {surface_text}, synonyms : {synonyms}, level : {level}, start_category : {start_category}, end_category : {end_category}, references: {references}, variants: {variants}, type: {types}\n"
            output.append(out_str)

        return "".join(output)
