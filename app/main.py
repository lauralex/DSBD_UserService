import datetime
import re

from fastapi import FastAPI, HTTPException

from app.kafka import consumers, producers
import app.db_utils.advanced_scheduler as scheduling
from app.models import User, BanUser, LimitResearches
import app.db_utils.mongo_utils as database

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


@app.post("/ban", response_model=User, response_model_include={'username', 'user_id', 'ban_period'})
async def ban_user(ban_user: BanUser):
    def parser_date(date):
        if date == 'perma':
            return datetime.datetime.max
        period = re.search(r'(\d)y(\d)m(\d)d', date)
        years = int(period.group(1))
        months = int(period.group(2))
        days = int(period.group(3))
        return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days + months * 30 + years * 365)

    if ban_user.period == 'perma':
        period = parser_date('perma')
    elif ban_user.period != 'null':
        period = parser_date(ban_user.period)
    else:
        period = None

    await database.mongo.db[User.collection_name].update_one({'username': ban_user.user}, {
        '$set': {
            'ban_period': period
        }
    })

    if (updated_user := await database.mongo.db[User.collection_name].find_one({'username': ban_user.user})) is not None:
        return updated_user
    return HTTPException(status_code=404, detail='User not found')


@app.post("/researches", response_model=User, response_model_include={'username', 'user_id', 'max_research'})
async def limit_researches(max_res: LimitResearches):
    await database.mongo.db[User.collection_name].update_one({'username': max_res.user}, {
        '$set': {
            'max_research': max_res.limit
        }
    })
    if (updated_user := await database.mongo.db[User.collection_name].find_one({'username': max_res.user})) is not None:
        return updated_user
    return HTTPException(status_code=404, detail='User not found')
