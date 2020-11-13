import sys
import json
from pymongo import MongoClient
from pyspark import SparkContext
from operator import add
from bson import json_util

search_keyword_list = ["Storm", "Winter", "Canada",
                       "Temperature", "Flu", "Snow", "Indoor", "Safety"]
client = MongoClient(
    'mongodb+srv://user:5408pass3@data-assignment3.xtllw.mongodb.net/RawDb?retryWrites=true&w=majority')
processed_data_dict = {}
count_keyword_list = ["Storm", "Winter", "Canada", "hot",
                      "cold", "Flu", "Snow", "Indoor",	"Safety", "rain", "ice"]

try:
    processed_database = client.get_database('ProcessedDb')
    for keyword in search_keyword_list:
        processed_records = processed_database[keyword]
        processed_data_cursor = processed_records.find()
        processed_data_list = json.loads(
            json_util.dumps(processed_data_cursor))
        processed_data_dict[keyword] = processed_data_list
except:
    print('load data error: ', sys.exc_info()[0])


try:
    processed_data_dict_str = json.dumps(processed_data_dict)
    processed_data_split = processed_data_dict_str.split()
    sc = SparkContext()
    tokenzied_rdd_twitter = sc.parallelize(processed_data_split)
    frequency_counts_twitter = tokenzied_rdd_twitter.flatMap(lambda x: x.split()).filter(
        lambda y: y in count_keyword_list).map(lambda word: (word, 1)).reduceByKey(add)
    output_twitter = frequency_counts_twitter.collect()
    print('------------------------------------------------')
    print('processed_data_split output', output_twitter)
    print('------------------------------------------------')
except:
    print('processed_data_split spark error: ', sys.exc_info()[0])


try:
    processed_reuter_database = client.get_database('ReuterDb')
    processed_reuter_records = processed_reuter_database.Reuter_Data
    processed_reuter_cursor = processed_reuter_records.find()
    processed_reuter_data_list_str = json_util.dumps(processed_reuter_cursor)
    processed_reuter_data_list_str = processed_reuter_data_list_str.replace(
        ".", " ")
    processed_reuter_data_list_str = processed_reuter_data_list_str.replace(
        ",", " ")
    reuter_text_split = processed_reuter_data_list_str.split()
    #sc = SparkContext()
    tokenzied_rdd_reuter = sc.parallelize(reuter_text_split)
    frequency_counts_reuter = tokenzied_rdd_reuter.flatMap(lambda x: x.split()).filter(
        lambda y: y in count_keyword_list).map(lambda word: (word, 1)).reduceByKey(add)
    output_reuter = frequency_counts_reuter.collect()
    print('------------------------------------------------')
    print('processed_reuter_data_list_str output', output_reuter)
    print('------------------------------------------------')
except:
    print('processed_reuter_data_list_str spark error: ', sys.exc_info()[0])
