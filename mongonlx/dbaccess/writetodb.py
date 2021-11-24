from mongonlx.config.mongoconn import db

collections = ["concepts", "real_concepts", "food_ingridients"]

concepts_col = db["food_ingridients"]

def insert_many_concepts(docs):
    concepts_col.insert_many(docs)

def insert_one_concept(doc):
    concepts_col.insert_one(doc)
