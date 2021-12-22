from mongonlx.config.mongoconn import db

def col_name_lang(col_name, lang_code):
    name = f"{col_name}_{lang_code}"
    return db[name]

# def insert_many_concepts(docs):
#     concepts_col.insert_many(docs)

def insert_one_concept(doc,colname,lang_code):
    col_name_lang(colname, lang_code).insert_one(doc)
 