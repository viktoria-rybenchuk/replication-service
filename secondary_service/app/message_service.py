import asyncio


class MessageManager:
    def __init__(self):
        self.messages = []
        self.lock = asyncio.Lock()

    def get_messages(self):
        return self.messages

    async def add_message(self, message):
        async with self.lock:
            self.messages.append(message)
            return message
