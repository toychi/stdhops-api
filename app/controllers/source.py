@app.route('/uploadfile', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def uploadfile():
    resp = make_response(jsonify({"Status":"Success"}), 200)
    return resp