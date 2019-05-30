''' controller and routes for houses '''
import os
from flask import request, jsonify, make_response, flash, redirect, url_for
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
import csv
from datetime import datetime, timezone
import calendar
from app.controllers import crossdomain
from app.schemas import validate_user
from bson.objectid import ObjectId
from flask_jwt_extended import (create_access_token, create_refresh_token,
                                jwt_required, jwt_refresh_token_required, get_jwt_identity)


def remove_outlier_price(list_price_size_tuple, sep=1.5):
    if len(list_price_size_tuple) < 2:
        return list_price_size_tuple
    
    prices = [x[0] for x in list_price_size_tuple]
    median = statistics.median(prices)
    std = statistics.stdev(prices)
    
    list_price_size_tuple_cleaned = [x for x in list_price_size_tuple if x[0] <= median + sep * median]
    
    return list_price_size_tuple_cleaned

def remove_outlier_size(list_price_size_tuple, sep=1.5 ):
    if len(list_price_size_tuple) < 2:
        return list_price_size_tuple
    
    sizes = [x[1] for x in list_price_size_tuple]
    median = statistics.median(sizes)
    
    list_price_size_tuple_cleaned = [x for x in list_price_size_tuple if x[1] <= median + sep * median]
    
    return list_price_size_tuple_cleaned


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
    for name in y['districts']:
        if name == "All":
            condos_id = [i['_id'] for i in mongo.db['condo'].find({}, {"_id":1})]
        else:
            condos_id = [i['_id'] for i in mongo.db['condo'].find({"district": name}, {"_id":1})]
        for year in range(y['startyear'], y['endyear'] + 1):
            for month, day in zip([1, 4, 7, 10], [31, 30, 30, 31]):
                pipeline = [
                    {
                        "$project": {
                            "property": 1,
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
                    }
                ]
                interval_docs = mongo.db[ptype].aggregate(pipeline)
                for name in y['districts']:
                    sale = 0
                    rent = 0
                    if name == "All":
                        condos_id = [i['_id'] for i in mongo.db['condo'].find({}, {"_id":1})]
                    else:
                        condos_id = [i['_id'] for i in mongo.db['condo'].find({"district": name}, {"_id":1})]
                    listing = [ doc for doc in list(mongo.db['condo_listing'].aggregate(pipeline)) if doc['property'] in condos_id]
                    for room_type in listing:
                        if "sale" in room_type.keys():
                            try:
                                sale += len(room_type['sale'])
                            except TypeError:
                                sale += 0
                        if "rent" in room_type.keys():
                            try:
                                rent += len(room_type['rent'])
                            except TypeError:
                                rent += 0
                    rent_volume[name].append(["{} {}".format(calendar.month_abbr[month], year), rent])
                    sale_volume[name].append(["{} {}".format(calendar.month_abbr[month], year), sale])

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
    rent_result = []
    sale_result = []

    pipeline = [
    {
        "$project": {
            "property": 1,
            "sale": {
                "$filter": {
                    "input": "$sale",
                    "as": "item",
                    "cond": { "$and": [
                        { "$gte": [ "$$item.daypost", datetime(y['startyear'], 1, 1) ] }, 
                        { "$lte": [ "$$item.daypost", datetime(y['endyear'], 12, 31) ] }
                    ] }
                }
            },
            "rent": {
                "$filter": {
                    "input": "$rent",
                    "as": "item",
                    "cond": { "$and": [
                        { "$gte": [ "$$item.daypost", datetime(y['startyear'], 1, 1) ] }, 
                        { "$lte": [ "$$item.daypost", datetime(y['endyear'], 12, 31) ] }
                    ] }
                }
            }
        }
    }]
    for name in mongo.db["condo"].distinct('district'):
        condos_id = [i['_id'] for i in mongo.db['condo'].find({"district": name}, {"_id":1})]
        listing = [ doc for doc in list(mongo.db['condo_listing'].aggregate(pipeline)) if doc['property'] in condos_id]
        sum_sale_price, sum_sale_size = 0, 0
        sum_rent_price, sum_rent_size = 0, 0

        for room_type in listing:
            if room_type['sale'] is not None:
                list_price_size_tuple = []
                for obj in room_type['sale']:
                    price = obj['price']
                    size = obj['size']
                    if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                        continue
                    price_size_tuple = (price,size)
                    list_price_size_tuple.append(price_size_tuple)
                
                list_price_size_tuple_cleaned = remove_outlier_size(remove_outlier_price(list_price_size_tuple, sep=0.75), sep=0.75)

                sum_sale_price += sum([x[0] for x in list_price_size_tuple_cleaned])
                sum_sale_size += sum([x[1] for x in list_price_size_tuple_cleaned])

            if room_type['rent'] is not None:
                list_rent_size_tuple = []
                for obj in room_type['rent']:
                    price = obj['price'] 
                    size = obj['size']
                    if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                        continue
                    price_size_tuple = (price, size)
                    list_rent_size_tuple.append(price_size_tuple)
                
                list_rent_size_tuple_cleaned = remove_outlier_size(remove_outlier_price(list_rent_size_tuple, sep=0.75), sep=0.75)
        
                sum_rent_price += sum([x[0] for x in list_rent_size_tuple_cleaned])
                sum_rent_size += sum([x[1] for x in list_rent_size_tuple_cleaned])

        if sum_sale_size > 0 and sum_rent_size > 0:
            sale_per_size = float(sum_sale_price / sum_sale_size)
            rent_per_size = float(sum_rent_price / sum_rent_size)
            sale_result.append({"name": name, "value": round(sale_per_size)})
            rent_result.append({"name": name, "value": round(rent_per_size)})

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


@app.route('/ratio2', methods=['OPTIONS', 'POST'])
@crossdomain(origin='*')
def ratio2():
    ptr_ratio = []
    y = request.get_json()
    ptype = 'condo_listing'
    condos = []

    for name in y['districts']:
        ratio = []
        if name == "All":
            condos_id = [i['_id'] for i in mongo.db['condo'].find({}, {"_id":1})]
        else:
            condos_id = [i['_id'] for i in mongo.db['condo'].find({"district": name}, {"_id":1})]
        for year in range(y['startyear'], y['endyear'] + 1):
            for month, day in zip([3, 6, 9, 12], [31, 30, 30, 31]):
                pipeline = [
                {
                    "$project": {
                        "property": 1,
                        "sale": {
                            "$filter": {
                                "input": "$sale",
                                "as": "item",
                                "cond": { "$and": [
                                    { "$gte": [ "$$item.daypost", datetime(year, 1, 1) ] }, 
                                    { "$lte": [ "$$item.daypost", datetime(year, month, day) ] }
                                ] }
                            }
                        },
                        "rent": {
                            "$filter": {
                                "input": "$rent",
                                "as": "item",
                                "cond": { "$and": [
                                    { "$gte": [ "$$item.daypost", datetime(year, 1, 1) ] }, 
                                    { "$lte": [ "$$item.daypost", datetime(year, month, day) ] }
                                ] }
                            }
                        }
                    }
                }]
                listing = [ doc for doc in list(mongo.db['condo_listing'].aggregate(pipeline)) if doc['property'] in condos_id]
                sum_sale_price, sum_sale_size = 0, 0
                sum_rent_price, sum_rent_size = 0, 0

                for room_type in listing:
                    if room_type['sale'] is not None:
                        list_price_size_tuple = []
                        for obj in room_type['sale']:
                            price = obj['price']
                            size = obj['size']
                            if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                                continue
                            price_size_tuple = (price,size)
                            list_price_size_tuple.append(price_size_tuple)
                        
                        list_price_size_tuple_cleaned = remove_outlier_size(remove_outlier_price(list_price_size_tuple, sep=0.75), sep=0.75)

                        sum_sale_price += sum([x[0] for x in list_price_size_tuple_cleaned])
                        sum_sale_size += sum([x[1] for x in list_price_size_tuple_cleaned])

                    if room_type['rent'] is not None:
                        list_rent_size_tuple = []
                        for obj in room_type['rent']:
                            price = obj['price'] 
                            size = obj['size']
                            if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                                continue
                            price_size_tuple = (price, size)
                            list_rent_size_tuple.append(price_size_tuple)
                        
                        list_rent_size_tuple_cleaned = remove_outlier_size(remove_outlier_price(list_rent_size_tuple, sep=0.75), sep=0.75)
                
                        sum_rent_price += sum([x[0] for x in list_rent_size_tuple_cleaned])
                        sum_rent_size += sum([x[1] for x in list_rent_size_tuple_cleaned])
                
                if sum_sale_size > 0 and sum_rent_size > 0:
                    sale_per_size = float(sum_sale_price / sum_sale_size)
                    rent_per_size = float(sum_rent_price / sum_rent_size)
                    ptr = sale_per_size / (rent_per_size * 12)
                    ratio.append([datetime(year, month - 1, 1).timestamp() * 1000 , ptr])
        ptr_ratio.append({
            "name": name,
            "data": ratio
        })
           
    resp = make_response(jsonify(ptr_ratio), 200)
    return resp


@app.route('/saledistribution', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def saledistribution():
    y = request.get_json()
    ptype = 'condo_listing'
    sum_sale_price = []
    sum_rent_price = []
    print(y)
    name = y['district']
    if name == "All":
        condos_id = [i['_id'] for i in mongo.db['condo'].find({}, {"_id":1})]
    else:
        condos_id = [i['_id'] for i in mongo.db['condo'].find({"district": name}, {"_id":1})]
    listing = [ doc for doc in list(mongo.db['condo_listing'].find({}, {"sale":1, "rent": 1, "property": 1})) if doc['property'] in condos_id]
    for room_type in listing:
        if "sale" in room_type.keys():
            list_price_size_tuple = []
            for obj in room_type['sale']:
                price = obj['price']
                size = obj['size']
                if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                    continue
                price_size_tuple = (price,size)
                list_price_size_tuple.append(price_size_tuple)
            
            list_price_size_tuple_cleaned = remove_outlier_price(list_price_size_tuple, sep=0.75)
            sum_sale_price.extend(list_price_size_tuple_cleaned)

        if "rent" in room_type.keys():
            list_rent_size_tuple = []
            for obj in room_type['rent']:
                price = obj['price'] 
                size = obj['size']
                if price == None or size == None or isinstance(size,str) or isinstance(price,str):
                    continue
                price_size_tuple = (price, size)
                list_rent_size_tuple.append(price_size_tuple)
            
            list_rent_size_tuple_cleaned = remove_outlier_price(list_rent_size_tuple, sep=0.75)
            sum_rent_price.extend(list_rent_size_tuple_cleaned)

    resp = make_response(
        jsonify({"rent_result": [ x[0] for x in remove_outlier_price(sum_rent_price)], "sale_result": [ x[0] for x in remove_outlier_price(sum_sale_price)]}), 200)
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

@app.route('/uploadfile', methods=['GET', 'POST', 'OPTIONS'])
@crossdomain(origin='*')
def uploadfile():
    if request.method == 'POST':
        print(request.files)
        if 'file' in request.files:
            mfile = request.files['file']
            mfile.save(os.path.join('./data', mfile.filename))
            with open(os.path.join('./data', mfile.filename)) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count = 0
                header = []
                for row in csv_reader:
                    if line_count == 0:
                        header = row[:]
                        line_count += 1
                    else:
                        dictionary = dict(zip(header, row))
                        mongo.db['userupload'].insert_one(dictionary)
                        line_count += 1
                print(f'Processed {line_count} lines.')
        else:
            return make_response(jsonify({"Status":"Bad request"}), 400)
    resp = make_response(jsonify({"Status":"Success"}), 200)
    return resp


@app.route('/rules', methods=['PUT', 'GET', 'POST', 'OPTIONS'])
@crossdomain(origin='*')
def rules():
    if request.method == 'GET':
        resp = make_response(jsonify({
            "status":"success",
            "rules": list(mongo.db['rules'].find({}))
            }), 200)
        return resp
    if request.method == 'POST':
        y = request.get_json()
        district = [d['name'] for d in y['district']]
        period = y['period']['pvalue']
        doc = {
            "value": y['value'],
            "type": y['type'],
            "district": district,
            "period": period,
            "operation": y['operation'],
            "condition": y['condition'],
            "amount": int(y['amount'])
        }
        mongo.db['rules'].insert_one(doc)
        resp = make_response(jsonify({"Status":"Success"}), 200)
        return resp
    if request.method == 'PUT':
        y = request.get_json()
        district = [d['name'] for d in y['district']]
        period = y['period']['pvalue']
        doc = {
            "value": y['value'],
            "type": y['type'],
            "district": district,
            "period": period,
            "operation": y['operation'],
            "condition": y['condition'],
            "amount": int(y['amount'])
        }
        mongo.db['rules'].update_one({"_id": ObjectId(y['_id'])}, {"$set": doc})
        resp = make_response(jsonify({"Status":"Success"}), 200)
        return resp

@app.route('/rules/<rule_id>', methods=['DELETE', 'OPTIONS'])
@crossdomain(origin='*')
def deleteRule(rule_id):
    mongo.db['rules'].delete_one({"_id": ObjectId(rule_id)})
    resp = make_response(jsonify({"Status":"Success"}), 200)
    return resp

