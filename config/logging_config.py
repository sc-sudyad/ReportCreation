from loguru import logger
import os
import sys

# Configure logger
log_dir = 'C:/Users/sudheer_yadav/PycharmProjects/ReportCreation/logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, 'app.log')

logger.remove()
logger.add(sys.stdout, format="{time} - {name} - {level} - {message}", level="INFO")
logger.add(
    log_file,
    rotation="00:00",
    retention=None,
    compression="zip",
    enqueue=True,
    level="INFO",
    format="{time} - {name} - {level} - {message}"
)