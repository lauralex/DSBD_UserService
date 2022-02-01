import datetime
import re

from fastapi import FastAPI, HTTPException

import app.db_utils.advanced_scheduler as scheduling
import app.db_utils.mongo_utils as database
from app.kafka import consumers, producers
from app.models import User

app = FastAPI()


@app.on_event("startup")
def run_consumers_producers_scheduler():
    scheduling.init_scheduler()
    consumers.init_consumers()
    producers.init_producers()


@app.on_event("shutdown")
def close_consumers():
    consumers.close_consumers()
    producers.close_producers()


@app.get("/users/")
async def users():
    # researches_count = await db['web_server_search'].count_documents({})
    # users_count = await db['web_server_user'].count_documents({})
    # if users_count == 0:
    #     return 'empty'
    # users = await db['web_server_user'].aggregate([
    #     {
    #         '$lookup': {
    #             'from': 'web_server_search',
    #             'localField': 'user_id',
    #             'foreignField': 'user_id',
    #             'as': 'search'
    #         }
    #     },
    #     {
    #         '$project': {
    #             '_id': False,
    #             'username': True,
    #             'count': {'$size': '$search'}
    #         }
    #     }
    # ]).to_list(None)
    #
    # return {'average': researches_count / users_count, 'users': users}
    user_list = await database.mongo.db[User.collection_name].find({}).to_list(length=None)
    return {'users': user_list, 'count': len(user_list)}


@app.put("/users/{username}/research-limit", response_model=User, response_model_exclude={'id'})
async def limit_user(username: str, limit: int):
    await database.mongo.db[User.collection_name].update_one({'username': username}, {
        '$set': {
            'max_research': limit if limit != 0 else None
        }
    })
    if (updated_user := await database.mongo.db[User.collection_name].find_one({'username': username})) is not None:
        return User.parse_obj(updated_user)
    return HTTPException(status_code=404, detail='User not found')

# def alca():
#     researches_count = await db['web_server_search'].count_documents({})
#     users_count = await db['web_server_user'].count_documents({})
#     if users_count == 0:
#         return 'empty'
#     users = await db['web_server_user'].aggregate([
#         {
#             '$lookup': {
#                 'from': 'web_server_search',
#                 'localField': 'user_id',
#                 'foreignField': 'user_id',
#                 'as': 'search'
#             }
#         },
#         {
#             '$project': {
#                 '_id': False,
#                 'username': True,
#                 'count': {'$size': '$search'}
#             }
#         }
#     ]).to_list(None)
#
#     return {'average': researches_count / users_count, 'users': users}


@app.put("/users/{username}/ban", response_model=User, response_model_exclude={'id'})
async def ban_user(username: str, ban_period: str):
    def parser_date(date):
        if date == 'perma':
            return datetime.datetime.max
        period = re.search(r'(\d)y(\d)m(\d)d', date)
        years = int(period.group(1))
        months = int(period.group(2))
        days = int(period.group(3))
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days + months * 30 + years * 365)

    if ban_period == 'perma':
        period = parser_date('perma')
    elif ban_period != 'null':
        period = parser_date(ban_period)
    else:
        period = None

    await database.mongo.db[User.collection_name].update_one({'username': username}, {
        '$set': {
            'ban_period': period
        }
    })

    if (updated_user := await database.mongo.db[User.collection_name].find_one({'username': username})) is not None:
        return User.parse_obj(updated_user)
    return HTTPException(status_code=404, detail='User not found')
