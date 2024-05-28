from jupiter_AI import JUPITER_LOGGER
from jupiter_AI.logutils import measure


@measure(JUPITER_LOGGER)
def access_db(mongo_url, db_name, collection_name, query=None):
    '''
    Function to Access MongodB database from code
    ----------
    Parameters
    ----------
    mongo_url       - mongo dB server url in the form of a string with port
                                  Eg.- 'localhost:27017'
    db_name         - Database Name - string
                                  Eg. - 'jupiter'
    collection_name - collection name string
                                      Eg. - 'JUP_DB_Dummy_Collection'
    query           - Default = None
                                      Else a dictionery of query to be done in the collection

    '''
    import pymongo
    import json
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    cursor = db[collection_name].find(query)
    docs = []
    for i in cursor:
        del i['_id']
        docs.append(i)
    client.close()
    return docs


@measure(JUPITER_LOGGER)
def group_db(mongo_url, db_name, collection_name, group_params):
    '''
    Function to access mongodB for gouping and aggregating
    This function we use to get distinct combinations of certain parameters in 
    a collection
    E.g - if group_params = ['pos','origin','destination','compartment']
    The output is an array that contains all the combinations of
    pos,origin,destination,compartment in the collection
    ----------
    Parameters
    ----------
    mongo_url       - mongo dB server url in the form of a string with port
                                  Eg.- 'localhost:27017'
    db_name         - Database Name - string
                                  Eg. - 'jupiter'
    collection_name - collection name string
                                      Eg. - 'JUP_DB_Dummy_Collection'
    group_params    - keys/fields in the collection for which we need the 
                                      unique combinations in the collection in the form of
                                      an array
                                      Eg. - ['pos','origin','destination','compartment']  
    '''
    import pymongo
    import json
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    dictionery = dict()
    for i in group_params:
        dictionery[i] = '$' + i
    pipe = [{"$group": {"_id": dictionery}}]
    cursor = db[collection_name].aggregate(pipeline=pipe)
    docs = []
    for i in cursor:
        docs.append(i['_id'])
    client.close()
    return docs


@measure(JUPITER_LOGGER)
def insert_to_db(mongo_url, db_name, collection_name, doc):
    '''
    Function to Update MongodB database from code
    ----------
    Parameters
    ----------
    mongo_url       - mongo dB server url in the form of a string with port
                                  Eg.- 'localhost:27017'
    db_name         - Database Name - string
                                  Eg. - 'jupiter'
    collection_name - collection name string
                                      Eg. - 'JUP_DB_Dummy_Collection'
    doc             - dictionery to be uploaded into the collection
    '''
    import pymongo
    import json
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    db[collection_name].insert_one(doc)
    client.close()
