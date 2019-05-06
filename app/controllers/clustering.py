import os
from flask import request, jsonify, make_response
from app import app, mongo, flask_bcrypt, jwt, mongo2
import pandas as pd
from pandas.io.json import json_normalize
import re
from collections import defaultdict
from sklearn import cluster
import statistics
import numpy as np
import geopandas as gpd
import pprint
import joblib
from datetime import datetime, timezone
import calendar
from app.controllers import crossdomain
from app.schemas import validate_user
from flask_jwt_extended import (create_access_token, create_refresh_token,
                                jwt_required, jwt_refresh_token_required, get_jwt_identity)


bkk_link = 'app/controllers/bkk_geo.json'
model_name = 'app/controllers/cluster_model.bin'
filenames = os.listdir('app/controllers/')
DISTRICTS_NAME = [
    "pn",
    "ds",
    "nc",
    "br",
    "bk",
    "bkp",
    "ptw",
    "ppstp",
    "pkn",
    "mbr",
    "lkb",
    "ynw",
    "sptw",
    "pyt",
    "tbr",
    "bky",
    "hk",
    "ks",
    "tlc",
    "bkn",
    "bkt",
    "pscr",
    "nk",
    "rbrn",
    "bp",
    "dd",
    "bku",
    "st",
    "bs",
    "ctc",
    "bkl",
    "pw",
    "kt",
    "sl",
    "ct",
    "dm",
    "rctw",
    "lp",
    "wtn",
    "bkh",
    "ls",
    "sm",
    "kny",
    "sps",
    "wtl",
    "ksw",
    "bn",
    "twwtn",
    "tk",
    "bb"
]

DISTRICTS_CODE = {
    "All": 0,
    "Bang Bon": 50,
    "Bang Kapi": 6,
    "Bang Khae": 40,
    "Bang Khen": 5,
    "Bang Kho Laem": 31,
    "Bang Khun Thian": 21,
    "Bang Khun Thain": 21,
    "Bang Na": 47,
    "Bang Phlat": 25,
    "Bang Rak": 4,
    "Bang Sue": 29,
    "Bangkok Noi": 20,
    "Bangkok Yai": 16,
    "Bueng Kum": 27,
    "Chatuchak": 30,
    "Chom Thong": 35,
    "Din Daeng": 26,
    "Don Mueang": 36,
    "Dusit": 2,
    "Huai Khwang": 17,
    "Khan Na Yao": 43,
    "Khlong Sam Wa": 46,
    "Khlong San": 18,
    "Khlong Toei": 33,
    "Lak Si": 41,
    "Lat Krabang": 11,
    "Lat Phrao": 38,
    "Min Buri": 10,
    "Nong Chok": 3,
    "Nong Khaem": 23,
    "Pathum Wan": 7,
    "Phasi Charoen": 22,
    "Phaya Thai": 14,
    "Phra Khanong": 9,
    "Phra Nakhon": 1,
    "Pom Prap Sattru Phai": 8,
    "Prawet": 32,
    "Rat Burana": 24,
    "Ratchathewi": 37,
    "Sai Mai": 42,
    "Samphanthawong": 13,
    "Saphan Sung": 44,
    "Sathon": 28,
    "Suan Luang": 34,
    "Taling Chan": 19,
    "Thawi Watthana": 48,
    "Thon Buri": 15,
    "Thung Khru": 49,
    "Wang Thonglang": 45,
    "Wang Thong Lang": 45,
    "Watthana": 39,
    "Yan Nawa": 12
}

DISTRICTS = [
"Phra Nakhon",
"Dusit",
"Nong Chok",
"Bang Rak",
"Bang Khen",
"Bang Kapi",
"Pathum Wan",
"Pom Prap Sattru Phai",
"Phra Khanong",
"Min Buri",
"Lat Krabang",
"Yan Nawa",
"Samphanthawong",
"Phaya Thai",
"Thon Buri",
"Bangkok Yai",
"Huai Khwang",
"Khlong San",
"Taling Chan",
"Bangkok Noi",
"Bang Khun Thian",
"Phasi Charoen",
"Nong Khaem",
"Rat Burana",
"Bang Phlat",
"Din Daeng",
"Bueng Kum",
"Sathon",
"Bang Sue",
"Chatuchak",
"Bang Kho Laem",
"Prawet",
"Khlong Toei",
"Suan Luang",
"Chom Thong",
"Don Mueang",
"Ratchathewi",
"Lat Phrao",
"Watthana",
"Bang Khae",
"Lak Si",
"Sai Mai",
"Khan Na Yao",
"Saphan Sung",
"Wang Thonglang",
"Khlong Sam Wa",
"Bang Na",
"Thawi Watthana",
"Thung Khru",
"Bang Bon"
]

@app.route('/mapcluster')
@crossdomain(origin='*')
def mapcluster():
    if 'cluster_model.bin' in filenames:
        kmcls = joblib.load(model_name)
        status = 'cache'
    else:
        collection_name = 'condo_listing'
        df = json_normalize(list(mongo.db[collection_name].find({})))
        sale_total = []
        rent_total = []
        size_total = []
        for ps, pr, ss in zip(df.sale.values, df.rent.values, df['size'].values):
            if type(ps) is not list:
                sale_total.append(0)
            else:
                sale_total.append(statistics.mean([v['price'] for v in ps]))
            if type(pr) is not list:
                rent_total.append(0)
            else:
                rent_total.append(statistics.mean([v['price'] for v in pr]))

            temp = [i for i in ss if type(i) is not str]
            if len(temp) < 1:
                size_total.append(0)
            else:
                size_total.append(statistics.mean(temp))
        df[['meanSale', 'meanRent', 'meanSize']] = pd.DataFrame(list(zip(sale_total, rent_total, size_total)))
        bkk = gpd.read_file(bkk_link)
        mydf = df.drop(['_id', 'name', 'rent', 'sale', 'size'], axis=1)
        g_k = mydf.groupby('location').mean()
        zdb = bkk.join(g_k, on='name')
        km = cluster.KMeans(n_clusters=5)
        kmcls = km.fit(zdb.fillna(0).drop(['name','th-name','code','hc-key','geometry'], axis=1).values)
        joblib.dump(kmcls, model_name)
        status = 'new'
    labels = list(map(int, kmcls.labels_))
    resp = make_response(
        jsonify({'labels': labels, 'keys': DISTRICTS_NAME, 'status': status, 'l': len(labels)}), 200)
    return resp


@app.route('/kmean', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def kmean():
    collection_name = 'condo_listing'
    df = json_normalize(list(mongo.db[collection_name].find({})))
    sale_total = []
    rent_total = []
    size_total = []
    for ps, pr, ss in zip(df.sale.values, df.rent.values, df['size'].values):
        if type(ps) is not list:
            sale_total.append(0)
        else:
            sale_total.append(statistics.mean([v['price'] for v in ps]))
        if type(pr) is not list:
            rent_total.append(0)
        else:
            rent_total.append(statistics.mean([v['price'] for v in pr]))

        temp = [i for i in ss if type(i) is not str]
        if len(temp) < 1:
            size_total.append(0)
        else:
            size_total.append(statistics.mean(temp))
    df[['meanSale', 'meanRent', 'meanSize']] = pd.DataFrame(list(zip(sale_total, rent_total, size_total)))
    mydf = df.drop(['_id', 'name', 'rent', 'sale', 'size', 'location'], axis=1)
    km = cluster.KMeans(n_clusters=5)
    kmcls = km.fit(mydf.values)

    latlng = {}
    for c in list(mongo.db['condo'].find({})):
        latlng[c['name']] = {
            "name": c['name'],
            "lat": c['location']['coordinates'][1],
            "lon": c['location']['coordinates'][0]
            }
    
    result = {}
    for i in range(5):
        result[str(i)] = []
        for count, label in enumerate(kmcls.labels_):
            if i == label:
                result[str(i)].append(latlng[df.iloc[count,:]['name']])

    resp = make_response(jsonify(result), 200)
    return resp


@app.route('/stat', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def stat():
    result = {}
    ptype = 'condo_listing'
    y = request.get_json()
    d = []
    pipeline = [
        {
            "$project": {
                "meanSale": { "$avg": "$sale.price" },
                "meanRent": { "$avg": "$rent.price" },
                "name": 1,
                "location": 1,
            }
        },
        {
            "$group": {
                "_id": "$location",
                "sale": {"$avg": "$meanSale"},
                "rent": {"$avg": "$meanRent"}
            }
        }
    ]
    query = list(mongo.db[ptype].aggregate(pipeline))
    for i in range(len(y['label'])):
        d_list = []
        for c in y['label'][i]['data']:
            d_list.append(DISTRICTS_NAME.index(c['code']))
        saleprice_list = []
        rentprice_list = []
        for j in query:
            if DISTRICTS.index(j["_id"])in d_list:
                if j["sale"] is not None:
                    saleprice_list.append(j["sale"])
                if j["rent"] is not None:
                    rentprice_list.append(j["rent"])
        saleprice_list = sorted(saleprice_list)
        rentprice_list = sorted(rentprice_list)
        saleprice_list = list(map(int, saleprice_list))
        rentprice_list = list(map(int, rentprice_list))
        # outliner = []
        # q1, q3 = np.percentile(price_list, [25, 75])
        # iqr = q3 - q1
        # lower_bound = q1 - (1.5 * iqr)
        # upper_bound = q3 + (1.5 * iqr)
        # # for p in price_list:
        # #     if p > lower_bound or p < upper_bound:
        # #         outliner.append(p)
        # outliner.append(q1)
        # outliner.append(q3)
        count = len(saleprice_list)
        mean1 = statistics.mean(saleprice_list) if len(saleprice_list) > 0 else 0
        mean2 = statistics.mean(rentprice_list) if len(rentprice_list) > 0 else 0
        result[y['label'][i]['name']] = {"Average Selling": round(mean1, 2), "Average Rental": round(
            mean2, 2), "count": count}
    resp = make_response(jsonify(result), 200)
    return resp
