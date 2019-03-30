''' controller and routes for houses '''
import os
from flask import request, jsonify, make_response
from app import app, mongo
import pandas as pd
from pandas.io.json import json_normalize
import re
from sklearn import cluster
import geopandas as gpd

bkk_link = 'app/controllers/bkk_geo.json'


@app.route('/house')
def house():
    data = list(mongo.db['houseeeee'].aggregate(
        [{"$group": {"_id": "$district", "count": {"$sum": 1}}}]))
    # data = mongo.db.houseeeee.find_one()
    resp = make_response(jsonify({'data': data}), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


@app.route('/volume')
def volume():
    # data = list(mongo.db['houseeeee'].aggregate([{"$match" : {"price":{'$regex' : 'ขาย'}}}]))
    sell_volume = mongo.db['houseeeee'].find(
        {"price": {'$regex': 'ขาย'}}).count()
    rent_volume = mongo.db['houseeeee'].find(
        {"price": {'$regex': 'ให้เช่า'}}).count()
    resp = make_response(
        jsonify({
            'sell': sell_volume,
            'rent': rent_volume
        }), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
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
def mapcluster():
    data = mongo.db.house.find()
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
    labels = list(map(int, km5cls.labels_))
    resp = make_response(
        jsonify({'labels': labels, 'keys': list(zdb['hc-key'])}), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
