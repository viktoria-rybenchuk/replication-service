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

        if replication_count == 1:
            logger.info(f"Message {msg.id} added, replicating asynchronously")
            task = asyncio.create_task(self._background_replicate_message(msg))
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
            return msg

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
        session: aiohttp.ClientSession,
        url: str,
        message: Message,
        timeout: int = 60,
    ) -> bool:
        endpoint = f"{url}/messages"
        try:
            logger.debug(f"Sending message {message.id} to {endpoint!r}")
            async with session.post(
                endpoint,
                json=message.model_dump(),  # ✅ конвертуємо тільки тут
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status == 200:
                    logger.info(f"Follower {url!r} acknowledged message {message.id}")
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
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(self._send_to_follower(session, url, message))
                for url in FOLLOWERS_URLS
            ]

            if replication_count == 2:
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                success = any(task.result() for task in done if not task.exception())

                for task in pending:
                    self.background_tasks.add(task)
                    task.add_done_callback(self.background_tasks.discard)

                return success

            elif replication_count == 3:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                successes = sum(1 for r in results if r is True)
                failures = len(results) - successes

                logger.info(
                    f"Replication summary for message {message.id}: "
                    f"{successes} succeeded, {failures} failed"
                )
                return all(r is True for r in results)

        return False

    async def _background_replicate_message(self, msg: Message) -> None:
        logger.info(
            f"Starting background replication for message {msg.id}: "
            f"{msg.content!r} to {len(FOLLOWERS_URLS)} followers"
        )

        try:
            success = await self._replicate_to_followers(msg, replication_count=3)
            if success:
                logger.info(f"Background replication complete for message {msg.id}")
            else:
                logger.error(f"Background replication FAILED for message {msg.id}")
        except Exception as e:
            logger.error(f"Background replication error for message {msg.id}: {e}")
