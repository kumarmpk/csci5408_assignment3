import re
import json
import sys
from pymongo import MongoClient

regular_expression = re.compile(
    r'\\u[0-9a-fA-F]{4}|\\U[0-9a-fA-F]{8}|https?://(?:[-w.]|(?:%[\da-fA-F]{2}))+|[!@#$]|//?|<.*?>|\\?')

client = MongoClient(
    'mongodb+srv://user:5408pass3@data-assignment3.xtllw.mongodb.net/RawDb?retryWrites=true&w=majority')
count_keyword_list = ["Storm", "Winter", "Canada", "hot",
                      "cold", "Flu", "Snow", "Indoor",	"Safety", "rain", "ice"]

try:
    reuter_data_list = []
    file_list = ['reut2-009.sgm', 'reut2-014.sgm']
    for file_name in file_list:
        with open(file_name, 'r') as sgm_file:
            complete_file = sgm_file.read()
            complete_file = complete_file.replace('\n', ' ')
            reuters_split_arr = re.findall(
                r'<REUTERS(.+?)</REUTERS>', complete_file)
            for reuter in reuters_split_arr:
                document_obj = {}

                title_list = re.findall(r'<TITLE>(.+?)</TITLE>', reuter)
                if len(title_list) > 0:
                    title_text = title_list[0]
                    title_text = regular_expression.sub(r'', title_text)
                else:
                    title_text = ''

                body_list = re.findall(r'<BODY>(.+?)</BODY>', reuter)
                if len(body_list) > 0:
                    body_text = body_list[0]
                    body_text = regular_expression.sub(r'', body_text)
                else:
                    body_text = ''

                date_list = re.findall(r'<DATELINE>(.+?)</DATELINE>', reuter)
                if len(date_list) > 0:
                    date_text = date_list[0]
                    date_text = regular_expression.sub(r'', date_text)
                else:
                    date_text = ''

                place_list = re.findall(r'<PLACES>(.+?)</PLACES>', reuter)
                if len(place_list) > 0:
                    place_list_with_d_tag = place_list[0]
                    new_places_arr = []
                    places_arr = re.findall(
                        r'<D>(.+?)</D>', place_list_with_d_tag)
                    for place in places_arr:
                        place = ' ' + place + ' '
                        place = regular_expression.sub(r'', place)
                        new_places_arr.append(place)
                else:
                    place_list_with_d_tag = ''

                if(body_text and body_text != ''):
                    document_obj = {
                        'title': title_text,
                        'body': body_text,
                        'date': date_text,
                        'places': new_places_arr
                    }
                    reuter_data_list.append(document_obj)

        print('reuter_data_list', len(reuter_data_list))

except TypeError as T:
    print('TypeError', T)
except:
    print("read reuter file error:", sys.exc_info()[0])


try:
    reuter_database = client.get_database('ReuterDb')
    processed_reuter_data_list = []
    reuter_records = reuter_database.Reuter_Data

    for record in reuter_data_list:
        record_str = json.dumps(record)
        record_str_1 = record_str.replace("\\\"", "")
        processed_reuter_data_str = regular_expression.sub(r'', record_str_1)
        processed_reuter_data = json.loads(processed_reuter_data_str)
        processed_reuter_data_list.append(processed_reuter_data)

    reuter_records.delete_many({})
    reuter_records.insert_many(processed_reuter_data_list)

    print('reuter data done')
except:
    print('inserting processed data error: ', sys.exc_info()[0])
