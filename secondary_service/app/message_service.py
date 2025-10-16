import asyncio
from typing import List

from .models import Message


class MessageManager:
    def __init__(self):
        self.messages: List[Message] = []
        self.lock = asyncio.Lock()
        self.message_ids: set[int] = set()
        self.last_record_id: int = 0

    def get_messages(self):
        return [msg.model_dump() for msg in self.messages]

    async def add_message(self, message):
        async with self.lock:
            msg_id = message.id
            if msg_id in self.message_ids:
                return

            if msg_id < self.last_record_id:
                self.messages.insert(msg_id - 1, message)
                self.message_ids.add(msg_id)
                return message

            self.messages.append(message)
            self.message_ids.add(msg_id)
            self.last_record_id = msg_id
            return message
