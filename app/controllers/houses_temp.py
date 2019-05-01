# @app.route('/volume', methods=['OPTIONS', 'POST'])
# # Split by year
    # for year in range(y["startyear"], y["endyear"] + 1):
    # pipeline = [
    #     {
    #         "$match": {
    #             "daypost": {"$gte": datetime(year), 1, 1), "$lte": datetime(year), 12, 31)},
    #             "location": {
    #                 '$in': y['districts']
    #             }
    #         }
    #     },
    #     {
    #         "$group": {
    #             "_id": "$location",
    #             "count": {"$sum": 1}
    #         }
    #     }
    # ]
    # rent_result=list(
    #     mongo.db['tgrent'].aggregate(pipeline))
    # sale_result=list(
    #     mongo.db['tgsale'].aggregate(pipeline))


# @app.route('/price', methods=['POST', 'OPTIONS'])
    # for year in range(y["startyear"], y["endyear"] + 1):
    #     pipeline=[
    #         {
    #             "$match": {
    #                 "listed_date": {"$gte": datetime(year, 1, 1), "$lte": datetime(year, 12, 31)}
    #             }
    #         },
    #         {
    #             '$group': {
    #                 '_id': {'c': '$district_code', 'd': '$district'},
    #                 'count': {'$sum': 1},
    #                 'total': {'$sum': '$priceint'}
    #             }
    #         },
    #         {
    #             '$addFields': {
    #                 'avgRent': {'$divide': ['$total', '$count']}
    #             }
    #         }
    #     ]
    #     result[year] = []
    #     for dtr in list(mongo.db['thaihometown_rent'].aggregate(pipeline)):
    #         temp = [district_name[dtr['_id']['c'] - 1], dtr['avgRent']]
    #         result[year].append(temp)