from fastapi import FastAPI, Depends
from pydantic import BaseModel
from typing import Union, Any, Annotated
from kafka import KafkaProducer
import json

app = FastAPI()


class UserTrackerRequest(BaseModel):
    user_id: int
    name: str
    message: Union[str, int]


class UserTrackerResponse(BaseModel):
    status: bool
    message: Union[str, int]


def get_kafka():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
    )


@app.post("/api/user-track")
def user_tracker(req: UserTrackerRequest, broker: Annotated[KafkaProducer, Depends(get_kafka)]) -> UserTrackerResponse:
    broker.send("user_tracker", value=bytes(json.dumps(req.__dict__).encode('utf-8')))

    return UserTrackerResponse(status=True, message=req.message)
