import asyncio
from asyncio import sleep
from typing import List

from .models import Message


class MessageManager:
    def __init__(self):
        self.messages: List[Message] = []
        self.lock = asyncio.Lock()
        self.message_ids: set[int] = set()
        self.last_record_id: int = 0
        self.postpone_messages = {}

    def get_messages(self):
        return [msg.model_dump() for msg in self.messages]

    async def add_message(self, message):
        expected_msg = self.last_record_id + 1
        async with self.lock:
            msg_id = message.id
            if msg_id in self.message_ids and msg_id == expected_msg:
                return

            if msg_id != expected_msg:
                self.add_missed_messages()

            if msg_id != expected_msg:
                self.postpone_messages[msg_id] = message
                self.message_ids.add(msg_id)
                return

            else:
                self.messages.append(message)
                self.message_ids.add(msg_id)
                self.last_record_id = msg_id
                return message



    def add_missed_messages(self):
        expected_msg_id = self.last_record_id + 1
        for i in range(len(self.postpone_messages)):
            if self.postpone_messages.get(expected_msg_id):
                self.messages.append(self.postpone_messages.get(expected_msg_id))
                self.last_record_id = expected_msg_id
                expected_msg_id += 1
