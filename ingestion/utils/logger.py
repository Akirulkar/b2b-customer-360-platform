import os
import sys
from loguru import logger

# Create logs directory if it does not exist
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Remove default handler
logger.remove()

# 1. Console Output (Colorized, clean format)
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
    colorize=True,
)

# 2. File Output (Daily rotation, compression, retention)
logger.add(
    os.path.join(LOG_DIR, "pipeline_{time:YYYY-MM-DD}.log"),
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
    rotation="00:00",       # Rotate daily at midnight
    retention="14 days",    # Keep logs for 14 days
    compression="zip",      # Compress old log files
    enqueue=True,           # Thread/process safe
    encoding="utf-8",
)

# Export the configured logger
__all__ = ["logger"]