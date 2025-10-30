from asyncio import sleep

from fastapi import FastAPI, HTTPException

from .models import MessageBody
from .monitoring import HeartbeatManager
from .replication_service import ReplicationManager
from .settings import CONFIG

app = FastAPI()


config = CONFIG

heartbeat_manager = HeartbeatManager(config)
replication_manager = ReplicationManager(heartbeat_manager, config)


@app.get("/messages")
def get_messages():
    messages = replication_manager.get_messages()
    return {"messages": messages}


@app.post("/messages")
async def add_message(message: MessageBody):
    try:
        message_body = message.model_dump()
        await replication_manager.replicate(
            message=message_body["content"], replication_count=message_body["w"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("startup")
async def on_startup():
    await sleep(10)
    return await heartbeat_manager.start()
