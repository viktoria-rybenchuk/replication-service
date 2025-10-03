from fastapi import FastAPI, Response, status

from .message_service import MessageManager
from .models import Message
from .utills import logger

app = FastAPI()
message_manager = MessageManager()


@app.post('/messages')
async def add_message(message: Message):
    try:
        await message_manager.add_message(message.model_dump())
        logger.info('Message successfully replicated')
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error('Failed to replicate message: %s', e)
        return Response(status_code=513)


@app.get('/messages')
def get_messages():
    return {'messages': message_manager.get_messages()}
