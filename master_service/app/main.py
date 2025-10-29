from fastapi import FastAPI

from .models import MessageBody
from .monitoring import HeartbeatManager
from .replication_service import ReplicationManager
from .settings import FOLLOWERS_CONFIG

app = FastAPI()


config = FOLLOWERS_CONFIG
heartbeat_manager = HeartbeatManager(config)
replication_manager = ReplicationManager(heartbeat_manager, config)


@app.get("/messages")
def get_messages():
    messages = replication_manager.get_messages()
    return {"messages": messages}


@app.post("/messages")
async def add_message(message: MessageBody):
    message_body = message.model_dump()
    await replication_manager.replicate(
        message=message_body["content"], replication_count=message_body["w"]
    )


@app.on_event("startup")
async def on_startup():
    return await heartbeat_manager.start()
