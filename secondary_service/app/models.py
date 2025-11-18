from pydantic import BaseModel


class Message(BaseModel):
    id: int
    content: str


class MessageBatch(BaseModel):
    messages: list[Message]
