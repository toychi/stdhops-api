''' controller and routes for houses '''
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
import joblib
from datetime import datetime, timezone
import calendar
from app.controllers import crossdomain
from app.schemas import validate_user
from flask_jwt_extended import (create_access_token, create_refresh_token,
                                jwt_required, jwt_refresh_token_required, get_jwt_identity)


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


@app.route('/volume', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def volume():
    rent_result = []
    sale_result = []
    rent_result_sum = []
    sale_result_sum = []
    y = request.get_json()
    ptype = 'condo_listing'
    rent_volume = defaultdict(list)
    sale_volume = defaultdict(list)
    for year in range(y['startyear'], y['endyear'] + 1):
        for month, day in zip([1, 4, 7, 10], [31, 30, 30, 31]):
            pipeline = [
                {
                    "$project": {
                        "name": 1,
                        "location": 1,
                        "sale": {
                            "$filter": {
                                "input": "$sale",
                                "as": "item",
                                "cond": { "$and": [
                                    { "$gte": [ "$$item.daypost", datetime(year, month, 1) ] }, 
                                    { "$lte": [ "$$item.daypost", datetime(year, month + 2, day) ] }
                                ] }
                            }
                        },
                        "rent": {
                            "$filter": {
                                "input": "$rent",
                                "as": "item",
                                "cond": { "$and": [
                                    { "$gte": [ "$$item.daypost", datetime(year, month, 1) ] }, 
                                    { "$lte": [ "$$item.daypost", datetime(year, month + 2, day) ] }
                                ] }
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "countSale": { "$cond": { "if": { "$isArray": "$sale" }, "then": { "$size": "$sale" }, "else": 0} },
                        "countRent": { "$cond": { "if": { "$isArray": "$rent" }, "then": { "$size": "$rent" }, "else": 0} },
                        "name": 1,
                        "location": 1
                    }
                },
                {
                    "$group": {
                        "_id": "$location",
                        "sumCountSale": { "$sum": "$countSale" },
                        "sumCountRent": { "$sum": "$countRent" }
                    }
                }
            ]
            all_rent = 0
            all_sale = 0
            for d in mongo.db[ptype].aggregate(pipeline):
                rent_volume[d["_id"]].append(["{} {}".format(calendar.month_abbr[month], year), d["sumCountRent"]])
                sale_volume[d["_id"]].append(["{} {}".format(calendar.month_abbr[month], year), d["sumCountSale"]])
                all_rent = all_rent + d["sumCountRent"]
                all_sale = all_sale + d["sumCountSale"]
            rent_volume["All"].append(["{} {}".format(calendar.month_abbr[month], year), all_rent])
            sale_volume["All"].append(["{} {}".format(calendar.month_abbr[month], year), all_sale])

    for district in y['districts']:
        sale_result.append({
            "id": district,
            "name": district,
            "data": sale_volume[district]
        })
        rent_result.append({
            "id": district,
            "name": district,
            "data": rent_volume[district]
        })
        sale_result_sum.append({
            "name": district,
            "drilldown": district,
            "y": sum([v for m, v in sale_volume[district]])
        })
        rent_result_sum.append({
            "name": district,
            "drilldown": district,
            "y": sum([v for m, v in rent_volume[district]])
        })

    resp = make_response(
        jsonify({
            "rent_result": rent_result, 
            "sale_result": sale_result,
            "rent_result_sum": rent_result_sum,
            "sale_result_sum": sale_result_sum
            }), 200)
    return resp


@app.route('/price', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def price():
    y = request.get_json()
    ptype = 'condo_listing'
    sale_pipeline = [
        { "$unwind": "$sale" },
        { 
            "$match": {
                "sale.daypost": { "$gte": datetime(int(y['startyear']), 1, 1), "$lte": datetime(int(y['startyear']), 12, 31) }
            }
        },
        {
            "$group": {
                "_id": "$location",
                'value': { "$avg": "$sale.price" }
            }
        },
        {
            "$addFields": {
                "name": "$_id",
            }
        }
    ]

    rent_pipeline = [
        { "$unwind": "$rent" },
        { 
            "$match": {
                "rent.daypost": { "$gte": datetime(int(y['startyear']), 1, 1), "$lte": datetime(int(y['startyear']), 12, 31) }
            }
        },
        {
            "$group": {
                "_id": "$location",
                'value': { "$avg": "$rent.price" }
            }
        },
        {
            "$addFields": {
                "name": "$_id",
            }
        }
    ]
    rent_result = list(mongo.db[ptype].aggregate(rent_pipeline))
    sale_result = list(mongo.db[ptype].aggregate(sale_pipeline))

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
    ptype = 'condo_listing'
    for d in y['districts']:
        ptr_ratio.append({
            "name": d,
            "data": []
        })

    for year in range(int(y["startyear"]), int(y["endyear"]) + 1):
        pipeline = [
        {
            "$project": {
                "name": 1,
                "location": 1,
                "sale": {
                    "$filter": {
                        "input": "$sale",
                        "as": "item",
                        "cond": { "$and": [
                            { "$gte": [ "$$item.daypost", datetime(year, 1, 1) ] }, 
                            { "$lte": [ "$$item.daypost", datetime(year, 12, 31) ] }
                        ] }
                    }
                },
                "rent": {
                    "$filter": {
                        "input": "$rent",
                        "as": "item",
                        "cond": { "$and": [
                            { "$gte": [ "$$item.daypost", datetime(year, 1, 1) ] }, 
                            { "$lte": [ "$$item.daypost", datetime(year, 12, 31) ] }
                        ] }
                    }
                }
            }
        },
        {
            "$project": {
                "meanSale": { "$avg": "$sale.price" },
                "meanRent": { "$avg": "$rent.price" },
                "name": 1,
                "location": 1,
                "sale": "$sale",
                "rent": "$rent"
            }
        },
        {
            "$addFields": {
                "ptr": { 
                    "$divide": ["$meanSale", { "$multiply": ["$meanRent", 12] }]
                }
            }
        },
        {
            "$group": {
                "_id": "$location",
                "meanPtr": { "$avg": "$ptr" }
            }
        }]

        ptr_result = {}
        for d in mongo.db[ptype].aggregate(pipeline):
            ptr_result[d["_id"]] = d["meanPtr"]
        try:
            ptr_result["All"] = statistics.mean([v for v in ptr_result.values() if v is not None])
        except statistics.StatisticsError:
            ptr_result["All"] = None

        for i in range(len(ptr_ratio)):
            district = ptr_ratio[i]['name']
            ptr = ptr_result.get(district, None)
            ptr_ratio[i]['data'].append([year, ptr])

    resp = make_response(jsonify(ptr_ratio), 200)
    return resp


@app.route('/saledistribution', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def saledistribution():
    y = request.get_json()
    ptype = 'condo_listing'
    sale_pipeline = [
        { "$unwind": "$sale" },
        {
            "$match": {
                "location": y['district']
            }
        },
        {
            "$project": {
                "_id": 0,
                "sale.price": 1
            }
        }
    ]
    rent_pipeline = [
        { "$unwind": "$rent" },
        {
            "$match": {
                "location": y['district']
            }
        },
        {
            "$project": {
                "_id": 0,
                "rent.price": 1
            }
        }
    ]
    rent_result = [doc['rent']['price'] for doc in mongo.db[ptype].aggregate(rent_pipeline)]
    sale_result = [doc['sale']['price'] for doc in mongo.db[ptype].aggregate(sale_pipeline)]

    resp = make_response(
        jsonify({"rent_result": rent_result, "sale_result": sale_result}), 200)
    return resp


@app.route('/location', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def location():
    y = request.get_json()
    result = []
    for c in mongo.db['condo'].find({"district": y['district']}):
        result.append({
            "name": c['name'],
            "lat": c['location']['coordinates'][1],
            "lon": c['location']['coordinates'][0]
        })
    resp = make_response(jsonify(result), 200)
    return resp
