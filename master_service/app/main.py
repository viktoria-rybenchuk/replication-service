from fastapi import FastAPI

from .models import Message
from .replication_service import ReplicationManager

app = FastAPI()
replication_manager = ReplicationManager()


@app.get('/messages')
def get_messages():
    messages = replication_manager.get_messages()
    return {'messages': messages}


@app.post('/messages')
async def add_message(message: Message):
    await replication_manager.add_message(message.model_dump())
