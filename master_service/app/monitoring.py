import asyncio
from dataclasses import dataclass
from datetime import datetime
import aiohttp

from .utills import logger


@dataclass
class ServiceStatus:
    url: str
    name: str
    last_heartbeat: datetime = None
    just_recovered: bool = False
    is_healthy: bool = True


class HeartbeatManager:
    def __init__(self, config, interval=2):
        self.config = config
        self.interval = interval
        self._running = False
        self.services = self._register_services()
        self.recovery_callback = None

    def _register_services(self):
        return {
            name: ServiceStatus(name=name, url=value.get("url"))
            for name, value in self.config.items()
        }

    def set_recovery_callback(self, callback):
        self.recovery_callback = callback

    def get_service_is_healthy(self, follower_name):
        return self.services[follower_name].is_healthy

    def get_just_recovered(self, follower_name):
        return self.services[follower_name].just_recovered

    def mark_success(self, service: ServiceStatus):
        if not service.is_healthy:
            service.just_recovered = True
            service.is_healthy = True
            service.last_heartbeat = datetime.now()
            logger.info(f"{service.name} recovered")
        else:
            service.just_recovered = False
            service.is_healthy = True
            service.last_heartbeat = datetime.now()

    def mark_failure(self, service: ServiceStatus):
        if service.is_healthy:
            logger.warning(f"{service.name} became unreachable")
        service.just_recovered = False
        service.is_healthy = False
        service.last_heartbeat = datetime.now()

    async def _send_heartbeat(self, service: ServiceStatus):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{service.url}/health",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        self.mark_success(service)
                        if service.just_recovered and self.recovery_callback:
                            logger.info(f"Triggering message recovery for {service.name}")
                            asyncio.create_task(self.recovery_callback(service.name))
                    else:
                        self.mark_failure(service)
        except Exception as e:
            self.mark_failure(service)

    async def _send_to_all(self):
        await asyncio.gather(
            *(self._send_heartbeat(s) for s in self.services.values()),
            return_exceptions=True
        )

    async def start(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._run())
            logger.info("Heartbeat sender started")

    async def _run(self):
        while self._running:
            await self._send_to_all()
            await asyncio.sleep(self.interval)
