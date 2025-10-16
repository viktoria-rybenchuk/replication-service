from pydantic import BaseModel


class MessageBody(BaseModel):
    content: str
    w: int


class Message(BaseModel):
    id: int
    content: str