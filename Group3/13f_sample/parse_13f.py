'''
parse html and save parsed data into mongodb
'''

import os
import json
from copy import deepcopy
from pymongo import MongoClient, TEXT
from utils import date_mapper, scrape_13f

# init database
DB_NAME = "13f"
client = MongoClient('localhost', 27017)
database_list = client.list_database_names()
if DB_NAME in database_list:
    client.drop_database(DB_NAME)
db = client[DB_NAME]
collection_hedgefund = db["hedgefund"]


with open("data/cusip2ticker.json", "r") as f:
    ticker_dic = json.load(f)

root_path = "/Users/ZZH/Northwestern/Research/hedgemind/data/13f/all_new"
companies = os.listdir(root_path)
companies = [company for company in companies if not company.startswith(".")]
all_hedgefund_item = []
for hedgefund in companies:
    hedgefund_item = {}
    hedgefund_name = hedgefund.replace("_", " ").replace(".", " ").lower().title()
    hedgefund_name = ' '.join(hedgefund_name.split())
    hedgefund_item['name'] = hedgefund_name
    hedgefund_item['data'] = {}

    print("Processing {}".format(hedgefund_name))

    file_path = os.path.join(root_path,hedgefund)
    file_list = os.listdir(file_path)
    file_list = [file for file in sorted(file_list) if not file.startswith(".")]
    data = [scrape_13f(os.path.join(file_path,file), ticker_dic) for file in file_list]


    for i, document in enumerate(data):
        if i == 0:
            continue
        document_time = date_mapper(document['DATE'][0])
        hedgefund_item['data'][document_time] = {}

        hedgefund_item['data'][document_time]['call_put'] = {'call':set(),'put':set()}

        # find all the tickers hold by the company in this or last period
        total_value = 0
        current_holding = {}
        last_holding = {}
        last_document = data[i - 1]
        for item in document.iterrows():
            ticker = ticker_dic[item[1]["CUSIP"]]

            if 'call' in item[1]["CALL_PUT"].lower():
                hedgefund_item['data'][document_time]['call_put']['call'].add(ticker)
            elif 'put' in item[1]["CALL_PUT"].lower():
                hedgefund_item['data'][document_time]['call_put']['put'].add(ticker)

            if ticker == "" or " " in ticker or "." in ticker:
                continue
            total_value += item[1]['VALUE']
            if ticker not in current_holding.keys():
                current_holding[ticker] = [item[1]['VALUE'], item[1]['SHARES'], item[1]['TITLE_OF_CLASS']]
            else:
                current_holding[ticker][0] += item[1]['VALUE']
                current_holding[ticker][1] += item[1]['SHARES']

        hedgefund_item['data'][document_time]['call_put']['call'] = list(hedgefund_item['data'][document_time]['call_put']['call'])
        hedgefund_item['data'][document_time]['call_put']['put'] = list(hedgefund_item['data'][document_time]['call_put']['put'])

        for item in last_document.iterrows():
            ticker = ticker_dic[item[1]["CUSIP"]]
            if ticker == "" or " " in ticker or "." in ticker:
                continue
            if ticker not in last_holding.keys():
                last_holding[ticker] = [item[1]['VALUE'], item[1]['SHARES'], item[1]['TITLE_OF_CLASS']]
            else:
                last_holding[ticker][0] += item[1]['VALUE']
                last_holding[ticker][1] += item[1]['SHARES']
        current_holding_tickers = set(current_holding.keys())
        last_holding_tickers = set(last_holding.keys())
        all_holdings = current_holding_tickers.union(last_holding_tickers)

        # put interested information into hedgefund_item
        hedgefund_item['data'][document_time]['holdings'] = {}
        hedgefund_item['data'][document_time]['values'] = {}
        hedgefund_item['data'][document_time]['holdings']['new_buy'] = {}
        hedgefund_item['data'][document_time]['holdings']['increased'] = {}
        hedgefund_item['data'][document_time]['holdings']['unchanged'] = {}
        hedgefund_item['data'][document_time]['holdings']['decreased'] = {}
        hedgefund_item['data'][document_time]['holdings']['sold_out'] = {}

        new_buy_value = 0
        increased_value = 0
        unchanged_value = 0
        decreased_value = 0
        sold_out_value = 0

        # Change rate
        hedgefund_item['data'][document_time]['change_rate'] = {}

        for holding in all_holdings:
            if holding in current_holding_tickers and holding not in last_holding_tickers:
                current_holding[holding].append("new_buy")
                hedgefund_item['data'][document_time]['holdings']['new_buy'][holding] = current_holding[holding]
                new_buy_value += current_holding[holding][0]
                hedgefund_item['data'][document_time]['change_rate'][holding] = 1
            elif holding not in current_holding_tickers and holding in last_holding_tickers:
                last_holding[holding].append("sold_out")
                hedgefund_item['data'][document_time]['holdings']['sold_out'][holding] = last_holding[holding]
                sold_out_value += last_holding[holding][0]
                hedgefund_item['data'][document_time]['change_rate'][holding] = -1
            else:
                # calculate change rate. In cases of number of shares equals to 0, use value to calculate
                if float(last_holding[holding][1]) != 0:
                    hedgefund_item['data'][document_time]['change_rate'][holding] = (current_holding[holding][1] - last_holding[holding][1]) / float(last_holding[holding][1])
                else:
                    hedgefund_item['data'][document_time]['change_rate'][holding] = (current_holding[holding][0] - last_holding[holding][0]) / float(last_holding[holding][0])

                if current_holding[holding][1] > last_holding[holding][1]:
                    current_holding[holding].append("increased")
                    hedgefund_item['data'][document_time]['holdings']['increased'][holding] = current_holding[holding]
                    increased_value += current_holding[holding][0]
                elif current_holding[holding][1] < last_holding[holding][1]:
                    current_holding[holding].append("decreased")
                    hedgefund_item['data'][document_time]['holdings']['decreased'][holding] = current_holding[holding]
                    decreased_value += current_holding[holding][0]
                else:
                    current_holding[holding].append("unchanged")
                    hedgefund_item['data'][document_time]['holdings']['unchanged'][holding] = current_holding[holding]
                    unchanged_value += current_holding[holding][0]
                    hedgefund_item['data'][document_time]['change_rate'][holding] = 0

        hedgefund_item['data'][document_time]['change_rate'] = {k: v for k, v in sorted(hedgefund_item['data'][document_time]['change_rate'].items(), key=lambda item: item[1], reverse=True)}
        hedgefund_item['data'][document_time]['holdings']['current_holdings'] = deepcopy(hedgefund_item['data'][document_time]['holdings']['new_buy'])
        hedgefund_item['data'][document_time]['holdings']['current_holdings'].update(hedgefund_item['data'][document_time]['holdings']['increased'])
        hedgefund_item['data'][document_time]['holdings']['current_holdings'].update(hedgefund_item['data'][document_time]['holdings']['unchanged'])
        hedgefund_item['data'][document_time]['holdings']['current_holdings'].update(hedgefund_item['data'][document_time]['holdings']['decreased'])


        assert new_buy_value + increased_value + unchanged_value + decreased_value == total_value
        hedgefund_item['data'][document_time]['values']['total'] = total_value
        hedgefund_item['data'][document_time]['values']['new_buy'] = new_buy_value
        hedgefund_item['data'][document_time]['values']['increased'] = increased_value
        hedgefund_item['data'][document_time]['values']['unchanged'] = unchanged_value
        hedgefund_item['data'][document_time]['values']['decreased'] = decreased_value
        hedgefund_item['data'][document_time]['values']['sold_out'] = sold_out_value


        # Portfolio weight
        Portfolio_weight = {}
        for holding in current_holding.keys():
            Portfolio_weight[holding] = current_holding[holding][0]/total_value
        for holding in hedgefund_item['data'][document_time]['holdings']['sold_out'].keys():
            Portfolio_weight[holding] = 0

        hedgefund_item['data'][document_time]['holdings']['all'] = deepcopy(hedgefund_item['data'][document_time]['holdings']['current_holdings'])
        hedgefund_item['data'][document_time]['holdings']['all'].update(hedgefund_item['data'][document_time]['holdings']['sold_out'])
        for holding in hedgefund_item['data'][document_time]['holdings']['all'].keys():
            hedgefund_item['data'][document_time]['holdings']['all'][holding].insert(0,Portfolio_weight[holding])
        hedgefund_item['data'][document_time]['holdings']['all'] = {k: v for k, v in sorted(hedgefund_item['data'][document_time]['holdings']['all'].items(), key=lambda item: item[1][0], reverse=True)}






    hedgefund_item['last_updated'] = sorted(list(hedgefund_item['data'].keys()))[-1] if len(hedgefund_item['data'].keys()) > 0 else ""
    all_hedgefund_item.append(hedgefund_item)

db["hedgefund"].create_index([('name', TEXT), ('manager', TEXT)], default_language='english')




