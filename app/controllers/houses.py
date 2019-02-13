''' controller and routes for houses '''
import os
from flask import request, jsonify, make_response
from app import app, mongo


@app.route('/house')
def house():
    data = mongo.db.house.find_one()
    resp = make_response(jsonify(data), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
