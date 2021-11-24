from pymongo import MongoClient
def get_database():
    HOST = "35.193.77.13"
    PORT = "27017"
    CONNECTION_STRING = f"mongodb://admin:******@{HOST}:{PORT}"
    client = MongoClient(CONNECTION_STRING)
    return client["WikiConcepts"]


db = get_database()
