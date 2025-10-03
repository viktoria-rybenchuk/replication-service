import logging
import os

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

service_name = os.getenv("SERVICE_NAME", "Follower")
logger = logging.getLogger(service_name)
