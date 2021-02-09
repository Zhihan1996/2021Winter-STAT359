import os
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import json
import urllib.request
import urllib.parse



def date_mapper(oridate):
    year = int(oridate.split("-")[0])
    month = int(oridate.split("-")[1])

    if month < 4:
        year -= 1
        date = [str(year), "12", "31"]
    elif month >= 4 and month < 7:
        date = [str(year), "03", "31"]
    elif month >= 7 and month < 10:
        date = [str(year), "06", "30"]
    elif month >= 10 and month < 13:
        date = [str(year), "09", "30"]
    else:
        raise ValueError("Date type is invalid")

    new_date = "-".join(date)
    return new_date



def get_data(PATH):
    files = os.listdir(PATH)
    files = [file for file in files if file.endswith(".html")]

    with open("data/cusip2ticker.json", "r") as f:
        ticker_dic = json.load(f)

    data = [scrape_13f(os.path.join(PATH, file), ticker_dic) for file in sorted(files)]
    return data

def map_jobs(jobs, openfigi_apikey = 'a87b9118-c579-4d6c-9e36-b44240e305e8'):
    '''
    Send an collection of mapping jobs to the API in order to obtain the
    associated FIGI(s).
    Parameters
    ----------
    jobs : list(dict)
        A list of dicts that conform to the OpenFIGI API request structure. See
        https://www.openfigi.com/api#request-format for more information. Note
        rate-limiting requirements when considering length of `jobs`.
    Returns
    -------
    list(dict)
        One dict per item in `jobs` list that conform to the OpenFIGI API
        response structure.  See https://www.openfigi.com/api#response-fomats
        for more information.
    '''
    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v2/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
    request.add_header('Content-Type','application/json')
    if openfigi_apikey:
        request.add_header('X-OPENFIGI-APIKEY', openfigi_apikey)
    request.get_method = lambda: 'POST'
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response code {}'.format(str(response.status_code)))
    return json.loads(connection.read().decode('utf-8'))

def scrape_13f(file_path, ticker_dic):

    html = open(file_path).read()
    soup = BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr')[11:]
    positions = []
    for row in rows:
        dic = {}
        position = row.find_all('td')
        dic["NAME_OF_ISSUER"] = position[0].text
        dic["TITLE_OF_CLASS"] = position[1].text
        dic["CUSIP"] = position[2].text
        dic["VALUE"] = int(position[3].text.replace(',', ''))*1000
        dic["SHARES"] = int(position[4].text.replace(',', ''))
        dic["CALL_PUT"] = position[6].text
        dic["DATE"] = file_path.split('/')[-1].strip(".html")
        try:
            dic["TICKER"] = ticker_dic[position[2].text].lower()
        except KeyError:
            # url = "http://quotes.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50&for=stock&submit=Search&by=cusip&criteria={}".format(position[2].text)
            # html = requests.get(url).text
            # soup = BeautifulSoup(html, 'lxml')
            # ticker_elem = soup.find('tr', attrs={"bgcolor": "#666666"})
            # ticker = ""
            # try:
            #     ticker = ticker_elem.next_sibling.next_sibling.find('a').text
            # except:
            #     pass
            jobs = [{'idType': 'ID_CUSIP', 'idValue': position[2].text}]
            job_results = map_jobs(jobs)
            try:
                ticker = job_results[0]['data'][0]['ticker']
            except:
                ticker = ""
            ticker_dic[position[2].text] = ticker
            dic["TICKER"] = ticker
            time.sleep(0.2)

        positions.append(dic)
    df = pd.DataFrame(positions)
    with open("data/cusip2ticker.json", "w") as f:
        json.dump(ticker_dic, f)
    return df


# with open("data/nlu_new.md", "a") as f:
#     for hedgefund in all_hedgefund_item:
#         f.write("## synonym:" + hedgefund['name'] + "\n")
#         synonym_list = [hedgefund['manager']]
#         if len(hedgefund['name'].split()) > 2:
#             synonym_list.append(' '.join(hedgefund['name'].split()[:-1]))
#         for synonym in synonym_list:
#             f.write("- "+synonym + "\n")
#         f.write("\n")

# from pymongo import MongoClient
# DB_NAME = "13f"
# client = MongoClient('localhost', 27017)
# db = client[DB_NAME]
# collection_hedgefund = db["hedgefund"]
# db_result = collection_hedgefund.find()
# all_hedgefund_item = []
# for re in db_result:
#     all_hedgefund_item.append(re)
#
# with open("data/holders.txt", "a") as f:
#     for hedgefund in all_hedgefund_item:
#         f.write(hedgefund['name'] + "\n")
#         f.write(hedgefund['name'].lower() + "\n")
#         f.write(hedgefund['name'].upper() + "\n")
#         f.write(hedgefund['manager'] + "\n")
#         f.write(hedgefund['manager'].lower() + "\n")
#         f.write(hedgefund['manager'].upper() + "\n")

# cusip_nums = set()
# for file in files:
#     cusip_nums = cusip_nums | set(scrape_13f(file).CUSIP)
#
# ticker_dic = {c:"" for c in cusip_nums}
# for c in list(ticker_dic.keys()):
#     url = "http://quotes.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50&for=stock&submit=Search&by=cusip&criteria={}".format(c)
#     html = requests.get(url).text
#     soup = BeautifulSoup(html, 'lxml')
#     ticker_elem = soup.find('tr', attrs={"bgcolor": "#666666"})
#     ticker = ""
#     try:
#         ticker = ticker_elem.next_sibling.next_sibling.find('a').text
#         ticker_dic[c] = ticker
#     except:
#         pass






# for company in companies:
#     company = company.replace(" ", "%2520")
#     target = "https://www.sec.gov/edgar/search/?r=el#/q=13f&dateRange=5y&startdt=2015-08-03&enddt=2020-08-03&category=all&locationType=located&locationCode=all&entityName={}".format(company)
#     req = urllib.request.Request(target,
#              headers={
#                  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
#              })
#     a = urllib.request.urlopen(req)
#     soup = BeautifulSoup(a)
#
# import csv
# with open("/Users/ZZH/Downloads/13f.tsv", "r", encoding="utf-8-sig") as f:
#     lines = list(csv.reader(f, delimiter=",", quotechar=None))
#
# miss_companies = []
# not_find_companies = []
# confused_companies = []
# success_companies = []
# for company in companies:
#     if company not in company_dict.keys():
#         miss_companies.append(company)
# for miss_company in miss_companies:
#     keyword = miss_company.split()[0]
#     # keyword = " ".join(keyword)
#     new_name = set()
#     for line in lines:
#         info = line[0].split("|")
#         if miss_company.lower() == info[1].lower() or keyword in info[1]:
#             new_name.add(info[1])
#     if len(new_name) < 1:
#         not_find_companies.append(miss_company)
#         print(miss_company)
#     elif len(new_name) == 1:
#         success_companies.append(new_name)
#         # miss_companies.remove(miss_company)
#     else:
#         confused_companies.append(miss_company)
#
# names = []
# for company in confused_companies:
#     new_name = set()
#     keyword = company.split()[0]
#     for line in lines:
#         info = line[0].split("|")
#         if company.lower() == info[1].lower() or keyword in info[1]:
#             new_name.add(info[1])
#     names.append(new_name)
#
# count_1 = 0
# count_2 = 0
# for line in lines:
#     info = line[0].split("|")
#     if len(info) > 3 and int(info[3].split("-")[0]) >= 2015:
#         if info[1] == 'ICAHN CARL C ET AL':
#             count_1+=1
#         elif info[1] == 'ICAHN CARL C':
#             count_2 +=1
# print(count_1)
#
# success_companies.append('ICAHN CARL C')
# confused_companies = confused_companies[1:]
# names = names[1:]
#
# for line in lines:
#     info = line[0].split("|")
#     if info[0] == "850529":
#         print(line)
#
# success_companies_cik = []
# for company in success_companies:
#     for line in lines:
#         info = line[0].split("|")
#         if info[1] == company:
#             success_companies_cik.append([company,info[0]])
#             break
#
# name_sets = []
# for company in confused_companies:
#     name_set = set()
#     for line in lines:
#         info = line[0].split("|")
#         if company in info[1]:
#             name_set.add(info[1])
#     name_set = list(name_set)
#     if len(name_set) == 1:
#         success_companies.append(name_set[0])
#         confused_companies.remove(company)
#     else:
#         name_sets.append(name_set)
#
# success_companies[0] = "Schonfeld Strategic Advisors LLC"
# new_companies = []
# for new_company in success_companies_cik:
#     new_companies.append(new_company[0])
# for company in success_companies:
#     if company not in new_companies:
#         print(company)
#
# a = ''.join(success_companies[0])


# Dow_30 = ["CSCO","KO","DIS","DOW","XOM","GS","HD","IBM","INTC","JNJ","JPM","MCD","MRK","MSFT","NKE","PFE","PG","TRV","UTX","UNH","VZ","V","WMT","WBA"]
# import json
# with open("data/ticker2com.json", "r") as f:
#     ticker2com = json.load(f)
#
# with open("nlu_new.md", "a") as f:
#     for ticker in ticker2com.keys():
#         if ticker in Dow_30:
#             continue
#         f.write("## synonym:{}".format(ticker) + "\n")
#         company_name = ticker2com[ticker].replace('.','').replace(',','')
#         f.write("- {}".format(ticker + "\n"))
#         f.write("- {}".format(ticker.lower() + "\n"))
#         f.write("- {}".format(company_name + "\n"))
#         f.write("- {}".format(company_name.lower()) + "\n")
#         if 'inc' in company_name.lower():
#             f.write("- {}".format(company_name.lower().replace("inc", "")) + "\n")
#             f.write("- {}".format(company_name.lower().replace("inc", "").title()) + "\n")
#         if 'lld' in company_name.lower():
#             f.write("- {}".format(company_name.lower().replace("lld", "")) + "\n")
#             f.write("- {}".format(company_name.lower().replace("lld", "").title()) + "\n")
#         f.write("\n")
#
#
#
# import json
# from pymongo import MongoClient
#
# DB_NAME = "13f"
# client = MongoClient('localhost', 27017)
# db = client[DB_NAME]
# collection_hedgefund = db["hedgefund"]
#
# result = collection_hedgefund.find()
# hedgefund_items = []
# for re in result:
#     hedgefund_items.append(re)
#
# with open("holders.txt", "w") as f:
#     for company in hedgefund_items:
#         names = [company['name'], company['manager']]
#         for name in names:
#             name = name.replace('.','').replace('/', '').split()
#             for i in range(len(name)):
#                 all_types = set()
#                 ori = ' '.join(name[:i+1])
#                 all_types.add(ori)
#                 all_types.add(ori.upper())
#                 all_types.add(ori.lower())
#                 all_types.add(ori.title())
#                 for item in all_types:
#                     f.write(item+"\n")
#
#
# import csv
# with open("master.tsv", "r", encoding="utf-8-sig") as f:
#     lines = list(csv.reader(f, delimiter="|", quotechar=None))
#
# form_13f = []
#
# for line in lines:
#     if "13F-HR" in line[2]:
#         form_13f.append(line)
#
# with open("13f.tsv", 'wt') as f:
#     w = csv.writer(f, delimiter='|')
#     for line in form_13f:
#         w.writerow(line)

import json
from pymongo import MongoClient

DB_NAME = "13f"
client = MongoClient('localhost', 27017)
db = client[DB_NAME]
collection_hedgefund = db["hedgefund"]

result = collection_hedgefund.find()
hedgefund_items = []
for re in result:
    re['_id'] = re['_id'].__str__()
    hedgefund_items.append(re)

with open("/Users/ZZH/Downloads/mgdb.json", "w") as f:
    json.dump(hedgefund_items,f)


