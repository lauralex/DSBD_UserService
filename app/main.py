from fastapi import FastAPI

from app.kafka import consumers, producers
import app.db_utils.advanced_scheduler as scheduling

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
