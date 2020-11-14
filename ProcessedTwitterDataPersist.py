import json
from pymongo import MongoClient
import re
from bson import json_util
import sys
from pymongo.errors import BulkWriteError
from json import JSONDecodeError

search_keyword_list = ["Storm", "Winter", "Canada",
                       "Temperature", "Flu", "Snow", "Indoor", "Safety"]
required_attribute_list = ['retweeted_status', 'in_reply_to_screen_name', 'in_reply_to_user_id', 'in_reply_to_status_id', 'iso_language_code', 'result_type', 'truncated', 'id_str', '$oid', '_id', 'created_at', 'id', 'text',
                           'metadata', 'user', 'retweet_count', 'favorite_count', 'lang', 'place', 'name', 'screen_name', 'location', 'followers_count', 'friends_count', 'statuses_count']
regular_expression = re.compile(
    r'\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8}|https?://(?:[-w.]|(?:%[\da-fA-F]{2}))+|[!@#$]|//?|<.*?>|\\?')

client = MongoClient(
    'mongodb+srv://user:5408pass3@data-assignment3.xtllw.mongodb.net/RawDb?retryWrites=true&w=majority')
rawdb_database = client.get_database('RawDb')


def process_user_obj(input):
    output_json = {}
    for key in input:
        if key in required_attribute_list:
            if type(input[key]) == str:
                output_json[key] = process_str(input[key])
            elif type(input[key]) == int:
                output_json[key] = input[key]
            elif type(input[key]) == bool:
                output_json[key] = str(input[key])
            elif type(input[key]) is None:
                output_json[key] = ''

    return output_json


def process_str(input):
    text = regular_expression.sub(r'', input)
    text = text.replace("\'", "")
    text = text.replace("\"", "")
    return text


def process_retweet_status(input):
    output_json = {}
    for key in input:
        if key in required_attribute_list:
            if type(input[key]) == dict:
                output_json[key] = process_user_obj(input[key])
            elif type(input[key]) == str:
                output_json[key] = process_str(input[key])
            elif type(input[key]) == int:
                output_json[key] = input[key]
            elif type(input[key]) == bool:
                output_json[key] = str(input[key])
            elif type(input[key]) is None:
                output_json[key] = ''

    return output_json


try:
    processed_data_str = ''
    processed_data_dict = {}
    for keyword in search_keyword_list:
        processed_data_list = []
        rawdb_records = rawdb_database[keyword]
        raw_data_cursor = list(rawdb_records.find())

        for record in raw_data_cursor:
            record_json = json.loads(json_util.dumps(record))
            processed_record_json = {}
            for key in record_json:
                if key in required_attribute_list:
                    if key == 'retweeted_status':
                        processed_record_json[key] = process_retweet_status(
                            record_json[key])
                    elif type(record_json[key]) == dict:
                        processed_record_json[key] = process_user_obj(
                            record_json[key])
                    elif type(record_json[key]) == str:
                        processed_record_json[key] = process_str(
                            record_json[key])
                    elif type(record_json[key]) == int:
                        processed_record_json[key] = record_json[key]
                    elif type(record_json[key]) == bool:
                        processed_record_json[key] = str(record_json[key])
                    elif type(record_json[key]) is None:
                        processed_record_json[key] = ''

            record_str = json.dumps(processed_record_json)
            record_str = record_str.replace('\n', ' ')
            processed_data_str = regular_expression.sub(
                r'', record_str)
            processed_data_str = processed_data_str.replace('\'', '\"')
            processed_data = json.loads(processed_data_str)
            processed_data_list.append(processed_data)

        processed_data_dict[keyword] = processed_data_list

        print('keyword: ', keyword)
        print('tweet processed data: ', len(processed_data_list))

except JSONDecodeError as e:
    print('json: ', e)
except:
    print("cleaning raw data error: ", sys.exc_info()[0])

try:
    for keyword in search_keyword_list:
        if len(processed_data_dict[keyword]) > 0:
            processed_database = client.get_database('ProcessedDb')

            processed_records = processed_database[keyword]

            processed_records.delete_many({})
            insert_list = processed_data_dict[keyword]
            processed_records.insert_many(insert_list)

            print('tweet processed db done.', keyword)

except BulkWriteError as e:
    print('BulkWriteError', e)
except:
    print('inserting processed data error: ', sys.exc_info()[0])
