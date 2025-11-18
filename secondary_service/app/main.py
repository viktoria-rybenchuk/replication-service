import os
from asyncio import sleep

from fastapi import FastAPI, Response, status

from .message_service import MessageManager
from .models import Message, MessageBatch
from .utills import logger

app = FastAPI()
message_manager = MessageManager()


@app.post("/messages")
async def add_message(message: Message):
    try:
        delay = os.getenv("DELAY")
        if delay:
            await sleep(float(delay))
        await message_manager.add_message(message)
        logger.info("Message successfully replicated")
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error("Failed to replicate message: %s", e)
        return Response(status_code=513)


@app.get("/messages")
def get_messages():
    return {"messages": message_manager.get_messages()}


@app.post("/health")
async def heartbeat():
    return Response(status_code=200)


@app.post("/messages/batch")
async def add_messages_batch(batch: MessageBatch):
    try:
        for message in batch.messages:
            await message_manager.add_message(message)

        logger.info(f"Batch of {len(batch.messages)} messages replicated")
        return Response(status_code=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Failed to replicate message batch: {e}")
        return Response(status_code=513)
