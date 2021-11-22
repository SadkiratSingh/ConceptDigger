'''
Created on 14-Jan-2015

@author: tushar@solutionbeyond.net
'''
from graph_tool.all import *
import pickle
import time
import re
import socket
import json
import copy

#CONFIGURE
address = "" #blank probably means 'listen on ALL IPs'
port = 5007
createFresh = True


# import sys;sys.path.append(r'/usr/lib64/python2.7/org.python.pydev_3.7.0.201408261926/pysrc')
# import pydevd
# 
# pydevd.settrace("61.12.32.122",True,True,5678)

# CONFIGURE
#dataFolderPrefix = '/media/sb/0ecc919c-e385-49d7-b59a-ec4dd463df75/'
#dataFolderPrefix = 'debug_data/'
dataFolderPrefix = 'new_debug_data/'

'''[pseudocode]
load dbpedia nodes (their synonyms and classification) from HDD file to RAM Tree object
'''

graph = Graph()
nodesDict = {}
name_map = graph.new_vertex_property("string")
synonyms_map = graph.new_vertex_property("vector<string>")

def insertNodes(fromFile, predicateKeyword):
    t2 = time.time()
    print ("processing " + fromFile + " ...")
    f = open(fromFile, 'r')
    for line in f.readlines():
        # if ntriple line is trying to tell about a new category
        if line.find(predicateKeyword)>0:
            #extract the subject - which is actually the child node
            subject = line[29:line.find(">",29)]
            #extract the object - which is actually the parent node
            obj = line[line.rfind("resource/")+9:line.rfind(">")]
            
            #if node is note already added in graph, then create a new node for it
            if subject not in nodesDict :
                node = graph.add_vertex()
                nodesDict[subject] = graph.vertex_index[node]
                name_map[node] = subject
                synonyms_map[node].append(convertEntityURItoLabel(subject))
            if obj not in nodesDict :
                node = graph.add_vertex()
                nodesDict[obj] = graph.vertex_index[node]
                name_map[node] = obj
                synonyms_map[node].append(convertEntityURItoLabel(obj))

            #add an edge to depict the child-parent relation
            childNode = nodesDict[subject]
            parentNode = nodesDict[obj]              
            graph.add_edge(graph.vertex(parentNode),graph.vertex(childNode))
    f.close()
    t3 = time.time()
    print ("processed " + fromFile + " in " + str(t3-t2) + " seconds") 

def convertEntityURItoLabel(entity_URI):
    return entity_URI.replace("_"," ").lower()

def assignSynonymFromLiteral(fromFile, predicateKeywords):
    t2 = time.time()
    print ("processing " + fromFile + " ...")
    
    i = len(nodesDict)

    f = open(fromFile, 'r')
    for line in f.readlines():
        for keyword in predicateKeywords:
            if line.find(keyword)>0:
                #extract the subject
                subject = line[29:line.find(">",29)]
                #extract the object
                searchObj = re.search('".*"', line) #searches for literal enclosed between "double quotes"
                obj = searchObj.group()[1:-1] #removes the trailing and beginning double quote character from literal
                obj = obj.lower()
                
                #p.write("subject = " + subject + ", obj = " + obj + "\n")
                if nodesDict.has_key(subject) == False :
                    nodesDict[subject] = i
                    label = convertEntityURItoLabel(subject)
                    graph.add_node(i, {"name":subject,"synonyms":[[label,fromFile]]})
                    i = i + 1
                    
                #if not obj in graph.node[nodesDict[subject]]['synonyms']:
                if [e for e in graph.node[nodesDict[subject]]['synonyms'] if e[0] == obj] == []:
                    graph.node[nodesDict[subject]]['synonyms'].append([obj,fromFile])
    f.close()
    t3 = time.time()
    print ("processed " + fromFile + " in " + str(t3-t2) + " seconds") 

def assignSynonymFromURI(fromFile, predicateKeywords):
    t2 = time.time()
    print ("processing " + fromFile + " ...")
    i = len(nodesDict)
    f = open(fromFile, 'r')
    #p = open("debug.log",'wb')

    for line in f.readlines():
        for keyword in predicateKeywords:
            if line.find(keyword)>0:
                #extract the subject
                subject = line[29:line.find(">",29)]
                #extract the object
                obj = line[line.rfind("resource/")+9:line.rfind(">")]

#                 try:
                #extract label of URI
                if nodesDict.has_key(subject) == False :
                    nodesDict[subject] = i
                    label = convertEntityURItoLabel(subject)
                    graph.add_node(i, {"name":subject,"synonyms":[[label,fromFile]]})
                    i = i + 1

                if nodesDict.has_key(obj) == False :
                    nodesDict[obj] = i
                    label = convertEntityURItoLabel(obj)
                    graph.add_node(i, {"name":obj,"synonyms":[[label,fromFile]]})
                    i = i + 1

                label = graph.node[nodesDict[subject]]['synonyms'][0][0]
                label = label.lower().replace(" (disambiguation)","")
                if [e for e in graph.node[nodesDict[obj]]['synonyms'] if e[0] == label] == []:
                    graph.node[nodesDict[obj]]['synonyms'].append([label,fromFile])
#                 except Exception,e:
#                     print 'couldnt write : ' + line + ".  because : " + str(Exception)
    f.close()
#     p.close()
    t3 = time.time()
    print ("processed " + fromFile + " in " + str(t3-t2) + " seconds") 

if createFresh == False:
    print ('Loading CACHE of graph data from hardisk..')
    t0 = time.time()
    graph = load_graph(dataFolderPrefix + "wiki_graph.gt")
    nodesDict = pickle.load (open(dataFolderPrefix + "nodesDict.pickle","rb"))
    t1 = time.time()
    print ("Time to load CACHE of graph data (from pickle files) = " + str(t1-t0))
else:
    t0 = time.time()
    i = 1
    insertNodes(dataFolderPrefix + "categories_skos.ttl", "/skos/core#broader")
    insertNodes(dataFolderPrefix + "categories_articles.ttl", "purl.org/dc/terms/subject")
    #assignSynonymFromLiteral(dataFolderPrefix + "labels_en.nt", ['http://www.w3.org/2000/01/rdf-schema#label'])
    #assignSynonymFromLiteral(dataFolderPrefix + "bold_keywords.nt", ['http://www.w3.org/2000/01/rdf-schema#label'])
    #assignSynonymFromLiteral(dataFolderPrefix + "mappingbased_properties_en.nt", ['http://dbpedia.org/property/name','http://dbpedia.org/property/alternateName','http://xmlns.com/foaf/0.1/name','http://dbpedia.org/ontology/alias'])
    #assignSynonymFromURI(dataFolderPrefix + "disambiguations_en.nt", ['http://dbpedia.org/ontology/wikiPageDisambiguates'])
    #assignSynonymFromURI(dataFolderPrefix + "redirects_en.nt", ['http://dbpedia.org/ontology/wikiPageRedirects'])
    t1 = time.time()
    print ("Graph loaded in : " + str(t1-t0) + " seconds")

#     print "processing skos_categories_en.nt"
#     thread.start_new_thread(insertNodes,(dataFolderPrefix + "skos_categories_en.nt", "/skos/core#broader"))
#     print "processing article_categories_en.nt"
#     thread.start_new_thread(insertNodes,(dataFolderPrefix + "article_categories_en.nt", "purl.org/dc/terms/subject"))
#       
#     print "processing mappingbased_properties_en.nt"
#     thread.start_new_thread(assignSynonymFromLiteral,(dataFolderPrefix + "mappingbased_properties_en.nt", ['http://dbpedia.org/property/name','http://dbpedia.org/property/alternateName','http://xmlns.com/foaf/0.1/name','http://dbpedia.org/ontology/alias']))
#     print "processing labels_en.nt"
#     assignSynonymFromLiteral(dataFolderPrefix + "labels_en.nt", ['http://www.w3.org/2000/01/rdf-schema#label'])
#     print "processing disambiguations_en.nt"
#     assignSynonymFromURI(dataFolderPrefix + "disambiguations_en.nt", ['http://dbpedia.org/ontology/wikiPageDisambiguates'])
#     print "processing redirects_en.nt"
#     assignSynonymFromURI(dataFolderPrefix + "redirects_en.nt", ['http://dbpedia.org/ontology/wikiPageDisambiguates'])


    print ("dumping pickle files")
    graph.vertex_properties["name-map"] = name_map
    graph.vertex_properties["synonyms-map"] = synonyms_map
    graph.save(dataFolderPrefix + "wiki_graph.gt")
    pickle.dump(nodesDict, open(dataFolderPrefix + "nodesDict.pickle","wb"),protocol=2)


'''[pseudocode]
enumerate all paths, that start from seed category to leaf nodes. maximum depth 5 (or n)
'''

maxDepth = 4
enumeratedChildInList = []

'''[pseudocode]
enumerate through all leaf nodes, so that their synonym information can be extracted
'''
# q = open('hierarchy_debug.log','wb')
# q.write('Parent, Child\n')
def enumerateChild(fromParent,curDepth,returnCategories,returnPages,seedCategoryParent,ancestors):
    parentNode = graph.vertex(fromParent)
    parentName = name_map[parentNode]
    successors = graph.iter_out_neighbors(parentNode)
    ancestors.append(parentName)
    a_list = copy.deepcopy(ancestors)
    for child in successors:
        childNodeData = {}
        childName = name_map[graph.vertex(child)]
        print(childName)
        if childName[0:9] == 'Category:':
            if curDepth < maxDepth: enumerateChild(child,curDepth+1,returnCategories,returnPages,seedCategoryParent,ancestors)
            if returnCategories==1:
                childNodeData["index"] = child
                childNodeData["name"] = childName
                childNodeData["level"] = curDepth
                childNodeData["ancestors"] = a_list
                enumeratedChildInList.append(childNodeData)
        else:
            if returnPages==1:
                childNodeData["index"] = child
                childNodeData["name"] = childName
                childNodeData["level"] = curDepth
                childNodeData["ancestors"] = a_list
                enumeratedChildInList.append(childNodeData)
    ancestors.pop()

#         if childName[0:9] == 'Category:':
#             if returnCategories==1:
#                 if any(e[1] == child for e in enumeratedChildInList) == False: #checks whether 'child' is already a member of enumeratedChildInList. It must be added only if its already not there
#                     enumeratedChildInList.append([child,seedCategoryParent])
#             if curDepth < maxDepth: enumerateChild(child,curDepth+1,returnCategories,returnPages,seedCategoryParent)
#         else:
#             if returnPages==1:
#                 if any(e[1] == child for e in enumeratedChildInList) == False: #checks whether 'child' is already a member of enumeratedChildInList. It must be added only if its already not there
#                     enumeratedChildInList.append([child,seedCategoryParent])

'''[pseudocode]
accept seed category under which child pages and their synonyms are to be extracted
'''
            
# #This class will handles any incoming request from
# #the browser 
# class httpHandler(BaseHTTPRequestHandler):
#      
#     #Handler for the GET requests
#     def do_GET(self):
#         print "self.path  = " + self.path 
#         self.send_response(200)
#         self.send_header('Content-type','text/html')
#         self.end_headers()
#         # Send the html message
#         self.wfile.write("Hello World !")
#         return
#  
# try:
#     #Create a web server and define the handler to manage the
#     #incoming request
#     server = HTTPServer(('', 5008), httpHandler)
#     print 'Started httpserver on port ' , 5008
#      
#     #Wait forever for incoming htto requests
#     server.serve_forever()
#  
# except KeyboardInterrupt:
#     print '^C received, shutting down the web server'
#     server.socket.close()

# @profile
def prepareOutput():
    output = []
    print(len(enumeratedChildInList))
    for child in enumeratedChildInList:
#         entity_url = graph.node[child[0]]['name']
#         seed_category = graph.node[child[1]]['name']
#         for synonym in graph.node[child[0]]['synonyms']:
        entity_url = child["name"]
        seed_category = child["name"]
        level = child["level"]
        ancestors = child["ancestors"]
        synonyms = synonyms_map[graph.vertex(child["index"])]
        output.append('{{"entity_url" : "DBPedia>{0}","surface_text" : "{1}","seed_category" : "{2}","level" : "{3}","ancestors" : "{4}"}}\n'.format(entity_url, synonyms, seed_category, level, ancestors))
#             output = (output + '{"entity_url" : "DBPedia>' + entity_url + '","surface_text" : "' + surface_text  + '","seed_category" : "' + seed_category  + '","how_this_record" : "' + how_this_record + '"}\n')
    return "".join(output)

sok = socket.socket()
sok.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sok.bind((address, port))
sok.listen(5)

print ('DBPedia Category & Page Digger & Synonym finder is listening on ' + address + ':' + str(port), '\nPress Ctrl+C to stop the server.')

while True:
    conn, addr = sok.accept()
    packet = conn.recv(10240)
#         try:
    categories_to_dig = json.loads( packet )

    enumeratedChildInList = [] #empty the list of children. TODO: than keeping it as a global variable we need to put this in a class, otherwise when users increase and we get multiple requests at the same time, the data of different users will merge into one

    '''[pseudocode]
    loop through each subcategory which is to be dug
    '''
    t0 = time.time()
    for subcategory in categories_to_dig: 
        seedCategory = subcategory[0]
        maxDepth =  subcategory[1]
        returnCategories = subcategory[2]
        returnPages = subcategory[3]
        print ('seedCategory = ' + seedCategory)
        print ('maxDepth = ' + str(maxDepth))
    
        '''[pseudocode]
        locate seed category in tree
        '''
        seedCategoryNode = nodesDict[seedCategory]
    
        #enumerate through all leaf nodes
        enumerateChild(seedCategoryNode,0,returnCategories,returnPages, seedCategoryNode,[])
    t1 = time.time()
    print ("Children enumerated in " + str(t1-t0) + " seconds")
    
    '''[pseudocode]
    extract synonyms of each node. return JSON file which can be imported in CP to train our Noisy NER
    '''
    t0 = time.time() 
    output = prepareOutput()  
    print(output)
    output_file = open(dataFolderPrefix + "output.txt", "w");
    output_file.write(output)
    output_file.close()           
    conn.send(output.encode()) 
    conn.close()
    t1 = time.time()
    print ("Reply sent in " + str(t1-t0) + " seconds")
#     f = open('output.txt','wb')
#     f.write(output)
#     f.close()
#         
# #         except Exception,e:
# #             print Exception.message
# except KeyboardInterrupt:
#     print '\nShutting down. Make a take a few seconds/minutes to free up all the memory..'
#     exit()

