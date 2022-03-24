from pymongo import MongoClient
import json
import datetime

client = MongoClient(host="localhost", port=27017)
hw9_db = client["HW9"]  # creat and connect to database
# new_collection = hw9_db.create_collection("personal_info") #creat collection comment it to avoid recreating
my_collection = hw9_db["personal_info"]  # after creating collection we should call the collection

# def insert_data():
#     with open("data.json", "r", encoding="utf8") as my_file:
#         read = json.load(my_file)
#         for i in read:
#             yield i

# read data from the json file

# my_collection.insert_many(insert_data()) #insert data to the collection comment it to avoid repeating

# using count
counting = my_collection.count_documents({})
print(f"Number of documents : {counting}")

# Tell  cities with the most and least users >>>
result = my_collection.aggregate(
    [
        {
            "$group": {
                "_id": "$location.city",
                "count_users": {
                    "$count": {}
                }
            }
        },
        {
            "$sort": {
                "count_users": 1
            }}
    ])
result = list(result)  # because the type of result is CommandCursor we change it to list of dicts
most_number, most_city = result[-1]["count_users"], result[-1]["_id"]
least_number, least_city = result[0]["count_users"], result[0]["_id"]
print(f"Most :\n{most_number} users are from {most_city}\n"
      f"least :\n{least_number} users are from {least_city}")

# count users base on state >>>

result = my_collection.aggregate(
    [
        {
            "$group": {
                "_id": "$location.state",
                "users": {
                    "$count": {}
                }
            }
        }
    ]
)
print("\n<<<This is result of counting users base on their state>>>\n")
for i in result:
    print(i)

# This is categorizing  >>>
result = my_collection.aggregate([
    {
        "$facet": {
            "categorized_old": [
                {"$match": {"dob.age": {"$gt": 40}}},
                {"$group": {"_id": "$dob.age"}},
                {"$project": {"categorize": "old"}}
            ],
            "categorize_middle_age": [
                {"$match": {"dob.age": {"$gt": 16, "$lt": 40}}},
                {"$group": {"_id": "$dob.age"}},
                {"$project": {"categorize": "middle_age"}}
            ],

            "categorize_youth": [
                {"$match": {"dob.age": {"$lt": 25}}},
                {"$group": {"_id": "$dob.age"}},
                {"$project": {"categorize": "youth"}}
            ]
        }
    }
])

result = list(result)

print("\nold categorize\n")
old = result[0]["categorized_old"]
for i in old:
    print(i)

print("\nyouth categorize\n")
youth = result[0]["categorize_youth"]
for i in youth:
    print(i)

print("\ncategorize_middle_age\n")
middle_age = result[0]["categorize_middle_age"]
for i in middle_age:
    print(i)

# compare avg age of users' from tehran and other cities >>>

result_1 = my_collection.aggregate(
    [
        {"$group": {
            "_id": "$location.city",
            "avg_age": {"$avg": {"$sum": "$dob.age"}},
            "counter": {"$count": {}}
        }},
        {"$match": {"_id": "تهران"}}
    ]
)

result_2 = my_collection.aggregate(
    [
        {"$match": {"location.city": {"$not": {'$regex': "تهران"}}}},
        {"$group": {
            "_id": "other_cities",
            "avg_age": {"$avg": {"$sum": "$dob.age"}},
            "counter": {"$count": {}}
        }}
    ]
)

print("\n<<<This is result of comparing Tehran and other cities >>>\n")
result_1 = list(result_1)
print(result_1[0])  # result of tehran
result_2 = list(result_2)
print(result_2[0])  # result of other cities except tehran

result = my_collection.aggregate([
    {"$facet": {
        "tehran": [
            {"$group": {
                "_id": "$location.city",
                "avg_age": {"$avg": "$dob.age"},
                "counter": {"$count": {}}
            }},
            {"$match": {"_id": "تهران"}}
        ],
        "others": [
            {"$match": {"location.city": {"$not": {'$regex': "تهران"}}}},
            {"$group": {
                "_id": "other_cities",
                "avg_age": {"$avg": "$dob.age"},
                "counter": {"$count": {}}
            }}
        ]
    }}
])

for i in result:
    print(i)

result_06 = my_collection.aggregate([
    {"$project":
        {
            "age": "$dob.age",
            "state": {"$cond":
                          [{"$eq":
                                ["$location.city", "تهران"]},
                           'tehran_city', 'other_city']}}},
    {"$group":
         {"_id": "$state",
          'avg': {"$avg": "$age"}}}
])


print(list(result_06))
