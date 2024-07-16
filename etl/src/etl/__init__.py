import os
import sys
import logging
from pathlib import Path


# Setting path variables
PROJECT_PATH = os.environ["PROJECT_PATH"]
LOG_PATH = Path(os.path.join(PROJECT_PATH, "src/logs"))
CONFIG_PATH = Path(os.path.join(PROJECT_PATH, "src/etl/config.yaml"))
PROXIES_PATH = Path(os.path.join(PROJECT_PATH, "src/etl/proxies.txt"))
STORAGE_PATH = Path(os.path.join(PROJECT_PATH, "data"))
LOG_FILE_PATH = os.path.join(LOG_PATH, "running_logs.log")

# Setting logger's format string
logging_str = (
    "[%(asctime)s: %(lineno)d: %(name)s: %(levelname)s: %(module)s: %(message)s]"
)

# Checking and creating log directory if not exists
os.makedirs(LOG_PATH, exist_ok=True)

# Setting logging configuration
logging.basicConfig(
    level=logging.INFO,
    format=logging_str,
    handlers=[logging.FileHandler(LOG_FILE_PATH), logging.StreamHandler(sys.stdout)],
)

# Creating and provideing logger's instance
logger = logging.getLogger("logger")
