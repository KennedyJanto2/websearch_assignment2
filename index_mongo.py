from pymongo import MongoClient  # import mongo client to connect
import pprint
import datetime

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Creating an instance of mongoclient and informing the connection string
    client = MongoClient(host=['localhost:27017'])
    # Creating database
    db = client.library

    #Creating a dictionary and include our data
    document = {"_id": 1,
                "title": "Discovery",
                "date": datetime.datetime.strptime("2023-10-03T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.000Z"),
                }

    # Creating a document
    documents = db.documents
    # Inserting data
    documents.insert_one(document)
    # Fetching data
    pprint.pprint(documents.find_one())

    #updating data
    documents.update_one({"_id": 1}, {"$set": {"title": "Arizona"}})
    # Fetching data
    pprint.pprint(documents.find_one())

    #deleting data
    documents.delete_one({"_id": 1})
    # Fetching data
    pprint.pprint(documents.find_one())