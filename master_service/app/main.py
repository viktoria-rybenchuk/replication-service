from fastapi import FastAPI

from .models import MessageBody
from .replication_service import ReplicationManager

app = FastAPI()
replication_manager = ReplicationManager()


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
