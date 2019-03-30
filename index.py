""" index file for REST APIs using Flask """
import os
import sys
import requests
from flask import jsonify, request, make_response, send_from_directory

ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
os.environ.update({'ROOT_PATH': ROOT_PATH})
sys.path.append(os.path.join(ROOT_PATH, 'stdhops-api'))

from app import app

# Port variable to run the server on.
PORT = os.environ.get('PORT')

@app.errorhandler(404)
def not_found(error):
    """ error handler """
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.route('/')
def index():
    data = [
            ['s', 1000],
            ['nc', 2],
            ['br', 3],
            ['bk', 4],
            ['bkp', 5],
            ['ptw', 6],
            ['ppstp', 7],
            ['pkn', 8],
            ['mbr', 9],
            ['lkb', 10],
            ['ynw', 11],
            ['sptw', 12],
            ['pyt', 13],
            ['tbr', 14],
            ['bky', 15],
            ['hk', 16],
            ['ks', 17],
            ['tlc', 18],
            ['bkn', 19],
            ['bkt', 20],
            ['pscr', 21],
            ['nk', 22],
            ['rbrn', 23],
            ['bp', 24],
            ['dd', 25],
            ['bku', 26],
            ['st', 27],
            ['bs', 28],
            ['ctc', 29],
            ['bkl', 30],
            ['pw', 31],
            ['kt', 32],
            ['sl', 33],
            ['ct', 34],
            ['dm', 35],
            ['rctw', 36],
            ['lp', 37],
            ['wtn', 38],
            ['bkh', 39],
            ['ls', 40],
            ['sm', 41],
            ['kny', 42],
            ['sps', 43],
            ['wtl', 44],
            ['ksw', 45],
            ['bn', 46],
            ['twwtn', 47],
            ['tk', 48],
            ['bb', 49]]
    resp = make_response(jsonify({'bin':data}), 200)
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=int(PORT)) # Run the app