import json
import socket
import time

from classes.GraphDataProcessor import GraphDataProcessor


class Connector(GraphDataProcessor):
    def __init__(self, create_fresh:bool, datafolderprefix:str, language_code:str):
        super().__init__(create_fresh, datafolderprefix, language_code)
        self._address = "" 
        self._port = 5008
        self._sok = socket.socket()
        self.__build_socket()
        self.__start_listening()
    
    def __build_socket(self):
        self._sok.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sok.bind((self._address, self._port))
        self._sok.listen(5)
        print ('DBPedia Category & Page Digger & Synonym finder is listening on ' + self._address + ':' + str(self._port), '\nPress Ctrl+C to stop the server.')
    
    def __start_listening(self):
        nodesDict = self._nodesdict
        enumerateChild = self._enumerateChild
        prepareOutput = self._prepareOutput

        while True:
            conn, addr = self._sok.accept()
            packet = conn.recv(10240)
            categories_to_dig = json.loads( packet )

            t0 = time.time()
            for subcategory in categories_to_dig: 
                seedCategory = subcategory[0]
                colName = subcategory[1]
                maxDepth =  subcategory[2]
                returnCategories = subcategory[3]
                returnPages = subcategory[4]
                print ('seedCategory = ' + seedCategory)
                print ('maxDepth = ' + str(maxDepth))

                seedCategoryNode = nodesDict[seedCategory]
                self._maxDepth = maxDepth
                self._mongoColname = colName
                enumerateChild(seedCategoryNode,0,returnCategories,returnPages, seedCategoryNode,[])
            t1 = time.time()
            print ("Children enumerated in " + str(t1-t0) + " seconds")
            
            t0 = time.time() 
            output = prepareOutput()          
            conn.send(output.encode()) 
            conn.close()
            self._clearDataStructuresUsedInProcessing()
            t1 = time.time()
            print ("Reply sent in " + str(t1-t0) + " seconds")
