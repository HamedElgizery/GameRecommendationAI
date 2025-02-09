from igdb.wrapper import IGDBWrapper
import pymongo
from config import *
import json
from tqdm import tqdm, trange


def rename_id(item):
    item["_id"] = item.pop("id")
    return item


mongo_client = pymongo.MongoClient("mongodb://neuraplay.duckdns.org:27017")
print("SUCCESS")
wrapper = IGDBWrapper(CLIENT_ID, ACCESS_TOKEN)
db = mongo_client["igdb"]

#collection.create_index([("id", pymongo.ASCENDING)])

entities = ["genres", "keywords", "themes", "game_modes", "franchises"]

# Could take quite some time
# entities.append("games") 


for entity in entities:
    query = "fields *; where id > 0; limit 500; sort id asc;"
    collection = db[entity]
    #collection.create_index([("id", pymongo.ASCENDING)])
    print(f"getting records for {entity}")
    cnt = 0
    while True:
        print(f"Progress: {cnt}")
        entity_list = wrapper.api_request(entity, query)
        entity_list = json.loads(entity_list)

        if not len(entity_list):
            break
        cnt += len(entity_list)
        collection.insert_many(entity_list)
        id = entity_list[-1]["id"]
        query = f"fields *; where id > {id}; limit 500; sort id asc;"


# bar = tqdm(total=300_000)
# query = (
#     "fields *; where id > 0 & first_release_date > 946684799; limit 500; sort id asc;"
# )
# bar = tqdm(total=300_000)
# while True:
#     games_list = wrapper.api_request("games", query)
#     games_list = json.loads(games_list)

#     if not len(games_list):
#         break

#     collection.insert_many(games_list)
#     bar.update(500)
#     id = games_list[-1]["id"]
#     query = f"fields *; where id > {id} & first_release_date > 946684799; limit 500; sort id asc;"
