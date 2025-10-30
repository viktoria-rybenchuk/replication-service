import asyncio
from asyncio import sleep
from typing import List
import aiohttp

from .models import Message
from .utills import logger


class ReplicationManager:
    def __init__(self, heartbeat_service, config):
        self.followers = config['followers']
        self.write_quorum = config['write_quorum']
        self.max_retry_time = config['retry_times']
        self.messages: List[Message] = []
        self.next_id: int = 1
        self.lock = asyncio.Lock()
        self.heartbeat_service = heartbeat_service
        self.heartbeat_service.recovery_callback = self.handle_service_recovery
        self.failed_messages = {}


    async def handle_service_recovery(self, follower_name: str):
        logger.info(f"Service recovery triggered for follower: {follower_name}")
        failed_count = len(
            self.failed_messages.get(follower_name, {}).get('messages', {})
        )
        logger.info(
            f"Attempting to resend {failed_count} failed messages to {follower_name}"
        )
        await self.send_failed_messages_to_follower(follower_name)

    async def send_failed_messages_to_follower(self, follower_name):
        try:
            if follower_name not in self.failed_messages:
                logger.warning(f"No failed msgs for {follower_name}")
                return

            failed_msgs = self.failed_messages[follower_name]['messages']
            msg_count = len(failed_msgs)

            tasks = [
                asyncio.create_task(
                    self._send_to_follower(
                        url=self.failed_messages[follower_name]['url'],
                        message=message,
                        follower_name=follower_name,
                    )
                )
                for msg_id, message in failed_msgs.items()
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)
            success = sum(1 for r in results if r is True)

            if success == msg_count:
                logger.info(f"Resent {msg_count} to {follower_name}")
                self.failed_messages[follower_name]['messages'] = {}
            else:
                logger.warning(f"{follower_name}: {success}/{msg_count} ok")

        except Exception as e:
            logger.error(f"Resend failed {follower_name}: {e}")

    async def replicate(self, message: str, replication_count: int) -> Message:
        if self.heartbeat_service.get_alive_replicas() < self.write_quorum:
            raise Exception(f"Quorum not met: {self.heartbeat_service.get_alive_replicas()}/{self.write_quorum}")

        msg = await self.add_message(message)
        replication_count -= 1

        logger.info(
            f"Starting replication for message {msg.id}: {msg.content!r} "
            f"to {len(self.followers)} followers (level={replication_count})"
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

    async def _session(self, url, message: Message, timeout):
        endpoint = f"{url}/messages"
        logger.debug(f"POST {endpoint} msg={message.id}")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint,
                    json=message.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    if response.status == 200:
                        logger.info(f"{url} ack msg {message.id}")
                        return True
                    else:
                        logger.warning(f"{url} status {response.status}")
                        raise Exception(f"HTTP {response.status}")
        except asyncio.TimeoutError:
            logger.error(f"Timeout {url} msg {message.id}")
            raise
        except Exception as e:
            logger.error(f"Error {url}: {e}")
            raise

    async def send_message_with_retry(
        self, follower_name, url, message, timeout, delay, retry
    ):
        logger.info(
            f"Retry {retry}/{self.max_retry_time} {follower_name} msg {message.id}"
        )
        await sleep(delay)
        return await self._send_to_follower(
            follower_name, url, message, timeout, delay * 2, retry + 1
        )

    def _queue_failed_message(self, follower_name: str, url: str, message: Message):
        if follower_name not in self.failed_messages:
            self.failed_messages[follower_name] = {'url': url, 'messages': {}}

        self.failed_messages[follower_name]['messages'][message.id] = message
        logger.debug(f"Queued msg {message.id} for {follower_name}")

    def _remove_from_failed_queue(self, follower_name: str, message_id: int):
        if (
            follower_name in self.failed_messages
            and message_id in self.failed_messages[follower_name]['messages']
        ):
            del self.failed_messages[follower_name]['messages'][message_id]
            logger.info(f"Recovered {follower_name} msg {message_id}")

    async def _send_to_follower(
        self,
        follower_name: str,
        url: str,
        message: Message,
        timeout: int = 10,
        delay=1,
        retry=0,
    ) -> bool:
        if not self.heartbeat_service.get_service_is_healthy(follower_name):
            logger.warning(f"Unhealthy {follower_name}, queued msg {message.id}")
            self._queue_failed_message(follower_name, url, message)
            return False

        try:
            result = await self._session(url, message, timeout)
            if result:
                self._remove_from_failed_queue(follower_name, message.id)
            return result

        except Exception:
            if retry >= self.max_retry_time:
                logger.error(f"Max retry {follower_name} msg {message.id}")
                self._queue_failed_message(follower_name, url, message)
                return False

            logger.warning(f"Failed {follower_name} msg {message.id}, retrying")
            return await self.send_message_with_retry(
                follower_name, url, message, timeout, delay, retry
            )

    async def _replicate_to_followers(
        self,
        message: Message,
        replication_count: int,
    ) -> bool:
        logger.info(
            f"Replicating message to {len(self.followers)} followers (need {replication_count})"
        )

        tasks = [
            asyncio.create_task(
                self._send_to_follower(
                    url=value['url'], message=message, follower_name=name
                )
            )
            for name, value in self.followers.items()
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
