import asyncio
from typing import List
import aiohttp

from .models import Message
from .settings import FOLLOWERS_URLS
from .utills import logger


class ReplicationManager:
    def __init__(self):
        self.messages: List[Message] = []
        self.next_id: int = 1
        self.lock = asyncio.Lock()
        self.background_tasks: set = set()

    async def replicate(self, message: str, replication_count: int) -> Message:
        msg = await self.add_message(message)
        replication_count -= 1

        logger.info(
            f"Starting replication for message {msg.id}: {msg.content!r} "
            f"to {len(FOLLOWERS_URLS)} followers (level={replication_count})"
        )

        success = await self._replicate_to_followers(msg, replication_count)

        if success:
            logger.info(f"Replication complete for message {msg.id}")
            return msg
        else:
            error_msg = f"Replication FAILED for message {msg.id}"
            logger.error(error_msg)
            raise Exception(error_msg)

    async def add_message(self, content: str) -> Message:
        async with self.lock:
            msg = Message(id=self.next_id, content=content)
            self.messages.append(msg)
            self.next_id += 1
            return msg

    def get_messages(self) -> List[Message]:
        return self.messages

    async def _send_to_follower(
        self,
        url: str,
        message: Message,
        timeout: int = 60,
    ) -> bool:
        endpoint = f"{url}/messages"
        try:
            logger.debug(f"Sending message {message.id} to {endpoint!r}")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    json=message.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status == 200:
                        logger.info(
                            f"Follower {url!r} acknowledged message {message.id}"
                        )
                        return True
                    else:
                        logger.error(
                            f"Follower {url!r} rejected message {message.id} "
                            f"(HTTP {response.status})"
                        )
                        return False
        except asyncio.TimeoutError:
            logger.error(f"Timeout sending message {message.id} to {url!r}")
            return False
        except Exception as e:
            logger.error(f"Error sending message {message.id} to {url!r}: {e}")
            return False

    async def _replicate_to_followers(
        self,
        message: Message,
        replication_count: int,
    ) -> bool:
        logger.info(
            f"Replicating message to {len(FOLLOWERS_URLS)} followers (need {replication_count})"
        )

        tasks = [
            asyncio.create_task(self._send_to_follower(url, message))
            for url in FOLLOWERS_URLS
        ]

        if replication_count == 0:
            return True

        successes = 0

        for future in asyncio.as_completed(tasks):
            ok_response = await future

            if ok_response:
                replication_count -= 1
                successes += 1

            if replication_count == 0:
                logger.info(
                    f"Replication successful ({successes}/{len(tasks)} responded)"
                )
                return True

        logger.error(
            f"Replication failed ({successes}/{replication_count + successes} needed)"
        )
        return False
