''' controller and routes for houses '''
import os
from flask import request, jsonify, make_response
from app import app, mongo, flask_bcrypt, jwt, mongo2
import pandas as pd
from pandas.io.json import json_normalize
import re
from sklearn import cluster
import statistics
import numpy as np
import geopandas as gpd
import joblib
from datetime import datetime, timezone
from app.controllers import crossdomain
from app.schemas import validate_user
from flask_jwt_extended import (create_access_token, create_refresh_token,
                                jwt_required, jwt_refresh_token_required, get_jwt_identity)


bkk_link = 'app/controllers/bkk_geo.json'
model_name = 'app/controllers/cluster_model.bin'
filenames = os.listdir('app/controllers/')

district_name = [
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

district_code = {
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


@jwt.unauthorized_loader
def unauthorized_response(callback):
    return jsonify({
        'ok': False,
        'message': 'Missing Authorization Header'
    }), 401


@app.route('/register', methods=['POST'])
def register():
    ''' register user endpoint '''
    data = validate_user(request.get_json())
    if data['ok']:
        data = data['data']
        data['password'] = flask_bcrypt.generate_password_hash(
            data['password'])
        mongo.db.users.insert_one(data)
        return jsonify({'ok': True, 'message': 'User created successfully!'}), 200
    else:
        return jsonify({'ok': False, 'message': 'Bad request parameters: {}'.format(data['message'])}), 400


@app.route('/auth', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def auth_user():
    ''' auth endpoint '''
    data = validate_user(request.get_json())
    if data['ok']:
        data = data['data']
        user = mongo.db.users.find_one({'email': data['email']}, {"_id": 0})
        if user and flask_bcrypt.check_password_hash(user['password'], data['password']):
            del user['password']
            access_token = create_access_token(identity=data)
            refresh_token = create_refresh_token(identity=data)
            user['token'] = access_token
            user['refresh'] = refresh_token
            return jsonify({'ok': True, 'data': user}), 200
        else:
            return jsonify({'ok': False, 'message': 'invalid username or password'}), 401
    else:
        return jsonify({'ok': False, 'message': 'Bad request parameters: {}'.format(data['message'])}), 400


@app.route('/refresh', methods=['POST'])
@jwt_refresh_token_required
def refresh():
    ''' refresh token endpoint '''
    current_user = get_jwt_identity()
    ret = {
            'token': create_access_token(identity=current_user)
    }
    return jsonify({'ok': True, 'data': ret}), 200


@app.route('/house')
@crossdomain(origin='*')
@jwt_required
def house():
    data = list(mongo.db['houseeeee'].aggregate(
        [{"$group": {"_id": "$district", "count": {"$sum": 1}}}]))
    # data = mongo.db.houseeeee.find_one()
    resp = make_response(jsonify({'data': data}), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route('/volume', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def volume():
    rent_result = {}
    sale_result = {}
    y = request.get_json()

    for district in y['districts']:
        if district == "All":
            district = {"$regex":".+"}
        query = {
            "daypost": {"$gte": datetime(int(y['startyear']), 1, 1), "$lte": datetime(int(y['endyear']), 12, 31)},
            "location": district
        }
        if type(district) == dict:
            district = "All"
        rent_result[district] = mongo.db['tgrent'].find(query).count()
        sale_result[district] = mongo.db['tgsale'].find(query).count()

    resp = make_response(
        jsonify({"rent_result": rent_result, "sale_result": sale_result}), 200)
    return resp


@app.route('/price', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def price():
    y = request.get_json()
    pipeline = [
        {
            "$match": {
                "daypost": {"$gte": datetime(int(y['startyear']), 1, 1), "$lte": datetime(int(y['endyear']), 12, 31)},
                "ptype": y['ptype']
            }
        },
        {
            "$group": {
                "_id": "$location",
                'value': { "$avg": { "$toInt": { "$trim": { "input": "$price" } } } }
            }
        },
        {
            "$addFields": {
                "name": "$_id",
            }
        }
    ]
    rent_result = list(mongo.db['tgrent'].aggregate(pipeline))
    sale_result = list(mongo.db['tgsale'].aggregate(pipeline))

    resp = make_response(
        jsonify({"rent_result": rent_result, "sale_result": sale_result}), 200)
    return resp


@app.route('/ratio', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def ratio():
    rent_result = {}
    sale_result = {}
    ptr_ratio = []
    y = request.get_json()
    for d in y['districts']:
        ptr_ratio.append({
            "name": d,
            "data": []
        })

    for year in range(int(y["startyear"]), int(y["endyear"]) + 1):
        pipeline = [
            {
                "$match": {
                    "daypost": {"$gte": datetime(year, 1, 1), "$lte": datetime(year, 12, 31)},
                    "ptype": y['ptype']
                }
            },
            {
                "$group": {
                    "_id": "$location",
                    'value': { "$avg": { "$toInt": { "$trim": { "input": "$price" } } } }
                }
            }
            # {
            #     "$addFields": {
            #         "x": year,
            #         "y": "$value"
            #     }
            # }
        ]

        for d in mongo.db['tgrent'].aggregate(pipeline):
            rent_result[d["_id"]] = d["value"]
        for d in mongo.db['tgsale'].aggregate(pipeline):
            sale_result[d["_id"]] = d["value"]

        for i in range(len(ptr_ratio)):
            district = ptr_ratio[i]['name']
            selling = sale_result.get(district, 0)
            rental = rent_result.get(district, 0) * 12
            if selling != 0 and rental != 0:
                ptr_ratio[i]['data'].append([year, round(selling / rental, 2)])
            else:
                ptr_ratio[i]['data'].append([year, 0])

    # resp = make_response(
    #     jsonify({"rent_result": rent_result, "sale_result": sale_result, "ratio": ratio}), 200)
    resp = make_response(jsonify(ptr_ratio), 200)
    return resp


@app.route('/saledistribution', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def saledistribution():
    y = request.get_json()
    pipeline = [
        {
            "$match": {
                "district_code": y["districtcode"]
            }
        },
        {
            '$project': {
                'priceint': 1,
                'listed_date': 1
            }
        }
    ]
    result = list(mongo2.db['thaihometown_sale'].aggregate(pipeline))

    resp = make_response(jsonify(result), 200)
    return resp


@app.route('/rent')
def rent():
    data = list(mongo.db['rental_price'].aggregate([
        # {
        #     "$match": {2
        #         "price": {"$regex": "ให้เช่า"}
        #     }
        # },
        # {
        #     "$project": {
        #         "_id": 0,
        #         "id": 1,
        #         "district": 1,
        #         "priceint": {
        #             "$reduce": {
        #                 "input": {'$split': [{'$trim': {'input': "$price", 'chars': "ให้เช่า บาท/เดือน"}}, ',']},
        #                 'initialValue': '',
        #                 'in': {
        #                     '$concat': [
        #                         '$$value',
        #                         '$$this']
        #                 }
        #             }
        #         }
        #     }
        # },
        {
            "$group": {
                "_id": "$district",
                "avg_rental": {"$avg": {"$toInt": "$priceint"}}
            }
        }
    ]))
    resp = make_response(jsonify(data), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route('/mapcluster')
@crossdomain(origin='*')
def mapcluster():
    if 'cluster_model.bin' in filenames:
        km5cls = joblib.load(model_name)
        status = 'cache'
    else:
        data = mongo.db.houseeeee.find()
        k = json_normalize(list(data))
        k[['bedroom', 'bathroom', 'parking']] = pd.DataFrame(
            k.room.values.tolist())
        k[['lat', 'lng']] = pd.DataFrame(k.latlng.values.tolist())
        k['bedroom'] = k['bedroom'].str.replace('\D', '')
        k['bathroom'] = k['bathroom'].str.replace('\D', '')
        k['parking'] = k['parking'].str.replace('\D', '')
        k['price'] = k['price'].str.replace('\D', '')
        k.loc[k.area.str.contains('ตารางวา', na=False), 'area'] = pd.to_numeric(
            k.loc[k.area.str.contains('ตารางวา', na=False)]['area'].str.replace('\D', '')) * 4
        k.loc[k.area.str.contains('ตารางเมตร', na=False), 'area'] = pd.to_numeric(
            k.loc[k.area.str.contains('ตารางเมตร', na=False)]['area'].str.replace('\D', ''))
        k.loc[k.area.str.contains('ไร่', na=False), 'area'] = pd.to_numeric(
            k.loc[k.area.str.contains('ไร่', na=False)]['area'].str.replace('\D', '')) * 1600
        k = k.fillna(0)
        km5 = cluster.KMeans(n_clusters=5)
        k = k.drop(['_id', 'date', 'id', 'latlng', 'name', 'room'], axis=1)
        for tag in ('price', 'area', 'bedroom', 'bathroom', 'parking'):
            k[tag] = pd.to_numeric(k[tag])
        g_k = k.groupby('district').mean()
        bkk = gpd.read_file(bkk_link)
        zdb = bkk.join(g_k, on='th-name').dropna()
        km5cls = km5.fit(
            zdb.drop(['name', 'th-name', 'code', 'hc-key', 'geometry'], axis=1).values)
        joblib.dump(km5cls, model_name)
        status = 'new'
    labels = list(map(int, km5cls.labels_))
    resp = make_response(
        jsonify({'labels': labels, 'keys': district_name, 'status': status, 'l': len(labels)}), 200)
    return resp


@app.route('/stat', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def stat():
    result = {}
    y = request.get_json()
    d = []
    pipeline = [
            {
                "$group": {
                    "_id": "$location",
                    "price": { "$push": "$price"}
                    # 'average': { "$avg": "$priceint" },
                    # 'stdv': { "$stdDevPop": "$priceint" },
                    # 'count': { "$sum": 1 }
                }
            }
        ]
    query = list(mongo.db['tgsale'].aggregate(pipeline))
    for i in range(len(y['label'])):
        d_list = []
        for c in y['label'][i]['data']:
            d_list.append(district_name.index(c['code']) + 1)
        price_list = []
        for j in query:
            if district_code[j["_id"]] in d_list:
                price_list.extend(j["price"])
        price_list = sorted(price_list)
        price_list = list(map(int, price_list))
        outliner = []
        q1, q3 = np.percentile(price_list,[25,75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)
        # for p in price_list:
        #     if p > lower_bound or p < upper_bound:
        #         outliner.append(p)
        outliner.append(q1)
        outliner.append(q3)
        count = len(price_list)
        mean = statistics.mean(price_list)
        stdev = statistics.stdev(price_list)
        result[y['label'][i]['name']] = { "mean": round(mean,2), "stdev": round(stdev,2), "count": count, "outliner": iqr, 'min': min(price_list), 'max': max(price_list)}
    resp = make_response(jsonify(result), 200)
    return resp

