import asyncio
import aiohttp

from .settings import FOLLOWERS_URLS
from .utills import logger


class ReplicationManager:
    def __init__(self):
        self.messages = []
        self.next_id = 1
        self.lock = asyncio.Lock()

    async def add_message(self, message: dict):
        async with self.lock:
            msg = {'id': self.next_id, 'content': message['content']}
            self.next_id += 1
            self.messages.append(msg)

        logger.info(f'Starting replication for message {msg["id"]}: {msg["content"]!r} '
                    f'to {len(FOLLOWERS_URLS)} followers')

        success = await self.replicate_message(msg)
        if success:
            logger.info(f'Replication complete for message {msg["id"]} (all followers confirmed)')
            return msg
        else:
            logger.error(f'Replication FAILED for message {msg["id"]} (one or more followers down)')
            raise Exception('Failed to replicate to all followers')

    def get_messages(self):
        return self.messages

    async def send_message_to_follower(self, session: aiohttp.ClientSession, url: str, message: dict):
        try:
            logger.debug(f'Sending message {message["id"]} to {url!r}')
            async with session.post(url, json=message, timeout=15) as response:
                if response.status == 200:
                    logger.info(f'Follower {url!r} acknowledged message {message["id"]}')
                    return True
                else:
                    logger.error(f'Follower {url!r} rejected message {message["id"]} '
                                   f'(HTTP {response.status})')
                    return False
        except Exception as e:
            logger.error(f'Error sending message {message["id"]} to {url!r}: {e}')
            return False

    async def replicate_message(self, message: dict):
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.send_message_to_follower(session, f'{url}/messages', message)
                for url in FOLLOWERS_URLS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            successes = sum(1 for r in results if r is True)
            failures = len(results) - successes

            logger.info(f'Replication summary for message {message["id"]}: '
                        f'{successes} succeeded, {failures} failed')

            return all(r is True for r in results)
