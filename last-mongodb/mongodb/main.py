from dotenv import load_dotenv, find_dotenv
import os
from pymongo import MongoClient
import pprint
from bson.objectid import ObjectId
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb://localhost:27017"

client = MongoClient(connection_string)
# if we want mongodb locally we have to give sudo service mongod start

dbs = client.list_database_names()
# print(dbs)

test_db = client.test
collections = test_db.list_collection_names()
# print(collections)

def insert_test_doc():
    collection = test_db.test
    test_document = {
        "name" : "Tim",
        "type" : "Test"

    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

# insert_test_doc()
production = client.production
person_collection= production.person_collection

def create_document():
    first_names = ["chithra","kala","madhu","ashok","aswath"]
    second_names = ["kala","darshi","priya","nimmala","prem"]
    ages = [21,32,12,43,23]
    docs = []

    for first_names,second_names,ages in zip (first_names,second_names,ages):
        doc = {"first_name":first_names,"second_name":second_names,"age":ages}
        docs.append(doc)
    person_collection.insert_many(docs)

create_document()

printer =pprint.PrettyPrinter()
def find_all_people():
    people = person_collection.find()
    # print(list(people))

    for person in people :
        printer.pprint(person)
# find_all_people()

def find_time():
    # first name is = to time
    tim = person_collection.find_one({"first_name": "chandra"})
    printer.pprint(tim)
# find_time()

def count_all_people():
    count = person_collection.find().count()
    print("Number of people",count)

# count_all_people()
def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    # based on id get person
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id":_id})
    printer.pprint(person)
# get_person_by_id("65f416292358915ab0588dcd")

def get_age_range(min_age,max_age):
    query = {"$and" : [
                {"age" : {"$gte" : min_age}},
                {"age" : {"$gte" : min_age}}
            ]}
    
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)
# get_age_range(20,30)
def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    # all_updates = {
    #     "$set" : {"new_field" : True},
    #     "$inc" : {"age" : 1},
    #     "$rename" : {"first_name" : "first","second_name" : "second"}
    # }
    # person_collection.update_one({"_id" :_id},all_updates)
    person_collection.update_one({"_id" : _id},{"$unset" : {"new_field":""}})
def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    new_doc = {
        "first_name" : "new first name",
        "last_name" : "new last name",
        "age" : 100

    }

    person_collection.replace_one({"_id" : _id},new_doc)
# replace_one("")
# update_person_by_id("")

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id" : _id})
    # person_collection.delete_many({}) delete all


# delete_doc_by_id("65f416292358915ab0588dce")
#----------------------------------------#

address = {
    "_id" :  "65f416292358915ab0588dec",
    "street" : "bay street",
    "number" : 27034,
    "city" : "baireddypalli",
    "country" : "india",
    "zip" : "24523"
}
    
def add_address_embed(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id},{"$addToset" : {'addresses':address }})
add_address_embed("65f4375c2fca725e0a7f61dc",address)



def add_address_relationship(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    address =address.copy()
    address["owner_id"] = _id

    address_collection =  production.address
    address_collection.insert_one(address)
add_address_relationship("65f42c9615cd7b83c7379198",address)



