
import logging
import logging.handlers
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    # datefmt="[%X]",
)

def LoggingAddFileHandler(file_path):
    handler = logging.FileHandler(file_path, "a", encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

def LoggingAddTimedRotatingFileHandler(file_path, when="d", interval=1, backup_count=0):
    handler = logging.handlers.TimedRotatingFileHandler(file_path, when=when, interval=interval, backupCount=backup_count, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)