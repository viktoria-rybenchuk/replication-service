import asyncio
from asyncio import sleep
from typing import List
import aiohttp

from .models import Message, MessageBatch
from .utills import logger


class ReplicationManager:
    def __init__(self, heartbeat_service, config):
        self.followers = config['followers']
        self.write_quorum = config['write_quorum']
        self.max_retry_time = config['retry_times']
        self.timeout = config['timeout']
        self.messages: List[Message] = []
        self.next_id: int = 1
        self.lock = asyncio.Lock()
        self.heartbeat_service = heartbeat_service
        self.heartbeat_service.recovery_callback = self.handle_service_recovery
        self.failed_messages = {}
        logger.info(
            f"ReplicationManager init: {len(self.followers)} followers, quorum={self.write_quorum}"
        )

    async def handle_service_recovery(self, follower_name: str):
        logger.info(f"Recovery: {follower_name} back online")
        failed_count = len(
            self.failed_messages.get(follower_name, {}).get('messages', {})
        )
        if failed_count == 0:
            logger.info(f"Recovery: {follower_name} has no queued messages")
            return
        logger.info(f"Recovery: resending {failed_count} msgs to {follower_name}")
        await self.send_failed_messages_to_follower(follower_name)

    async def send_failed_messages_to_follower(self, follower_name):
        async with self.lock:
            try:
                if follower_name not in self.failed_messages:
                    logger.warning(f"No failed msgs for {follower_name}")
                    return

                failed_msgs = self.failed_messages[follower_name]['messages']
                follower_url = (
                    f"{self.failed_messages[follower_name]['url']}/messages/batch"
                )

                message_batch = MessageBatch(messages=list(failed_msgs.values()))
                logger.info(
                    f"Sending batch [{len(failed_msgs)} msgs] to {follower_name}"
                )

                try:
                    await self._session(follower_url, message_batch)
                    logger.info(f"Batch sent OK to {follower_name}")
                    del self.failed_messages[follower_name]
                except Exception as e:
                    logger.error(f"Batch failed to {follower_name}: {e}")

            except Exception as e:
                logger.error(f"Recovery error for {follower_name}: {e}")

    async def replicate(self, message: str, replication_count: int) -> Message:
        alive = self.heartbeat_service.get_alive_replicas()
        logger.info(f"Replicate request: alive={alive}/{self.write_quorum} required")

        if alive < self.write_quorum:
            logger.error(f"Quorum not met: {alive}/{self.write_quorum}")
            raise Exception(f"Quorum not met: {alive}/{self.write_quorum}")

        msg = await self.add_message(message)
        replication_count -= 1

        logger.info(
            f"Msg {msg.id} created, replicating to {replication_count} followers"
        )

        success = await self._replicate_to_followers(msg, replication_count)

        if success:
            logger.info(f"Msg {msg.id} replicated successfully")
            return msg
        else:
            logger.error(f"Msg {msg.id} replication FAILED")
            raise Exception(f"Replication FAILED for message {msg.id}")

    async def add_message(self, content: str) -> Message:
        async with self.lock:
            msg = Message(id=self.next_id, content=content)
            self.messages.append(msg)
            self.next_id += 1
            return msg

    def get_messages(self) -> List[Message]:
        return self.messages

    async def _session(self, url, message: Message | list[Message]):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url=url,
                    json=message.model_dump(),
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                ) as response:
                    if response.status == 200:
                        logger.debug(f"POST {url} => 200 OK")
                        return True
                    else:
                        logger.warning(f"POST {url} => {response.status}")
                        raise Exception(f"HTTP {response.status}")
        except asyncio.TimeoutError:
            logger.warning(f"Timeout: {url} ({self.timeout}s)")
            raise Exception(f"Timeout {url}")
        except Exception as e:
            logger.warning(f"Error: {url} - {e}")
            raise Exception(f"Error {url}: {e}")

    async def send_message_with_retry(self, follower_name, url, message, delay, retry):
        logger.info(
            f"Retry {retry}/{self.max_retry_time}: {follower_name} msg {message.id} (wait {delay}s)"
        )
        await sleep(delay)
        return await self._send_to_follower(
            follower_name, url, message, delay * 2, retry + 1
        )

    async def _queue_failed_message(
        self, follower_name: str, url: str, message: Message
    ):
        async with self.lock:
            if follower_name not in self.failed_messages:
                self.failed_messages[follower_name] = {'url': url, 'messages': {}}

            self.failed_messages[follower_name]['messages'][message.id] = message
            total_failed = len(self.failed_messages[follower_name]['messages'])
            logger.warning(
                f"Queued msg {message.id} for {follower_name} (total: {total_failed})"
            )

    async def _send_to_follower(
        self,
        follower_name: str,
        url: str,
        message: Message,
        delay=1,
        retry=0,
    ) -> bool:
        if not self.heartbeat_service.get_service_is_healthy(follower_name):
            logger.warning(f"Unhealthy {follower_name}, queued msg {message.id}")
            await self._queue_failed_message(follower_name, url, message)

        try:
            follower_url = f"{url}/messages"
            logger.debug(f"Sending msg {message.id} to {follower_name}")
            await self._session(follower_url, message)
            logger.info(f"Sent msg {message.id} to {follower_name}")
            return True
        except Exception as e:
            if retry >= self.max_retry_time:
                logger.error(
                    f"Max retries reached for {follower_name} msg {message.id}"
                )
                await self._queue_failed_message(follower_name, url, message)
                return False

            logger.warning(f"Failed {follower_name} msg {message.id}: {e}")
            return await self.send_message_with_retry(
                follower_name, url, message, delay, retry
            )

    async def _replicate_to_followers(
        self,
        message: Message,
        replication_count: int,
    ) -> bool:
        logger.info(
            f"Msg {message.id}: sending to {len(self.followers)} followers (need {replication_count} success)"
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
            logger.info(f"Msg {message.id}: no replication required")
            return True

        successes = 0

        for future in asyncio.as_completed(tasks):
            ok_response = await future

            if ok_response:
                replication_count -= 1
                successes += 1
                logger.debug(
                    f"Msg {message.id}: {successes} success, {replication_count} needed"
                )

            if replication_count == 0:
                logger.info(
                    f"Msg {message.id}: quorum met ({successes}/{len(tasks)} responded)"
                )
                return True

        logger.error(
            f"Msg {message.id}: quorum failed ({successes}/{replication_count + successes} needed)"
        )
        return False
