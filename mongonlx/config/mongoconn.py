from pymongo import MongoClient
def get_database():
    HOST = "127.0.0.1"
    PORT = "27019"
    CONNECTION_STRING = f"mongodb://{HOST}:{PORT}"
    client = MongoClient(CONNECTION_STRING)
    return client["WikiConcepts"]


db = get_database()
