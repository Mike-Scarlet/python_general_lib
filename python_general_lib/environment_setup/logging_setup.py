
import logging
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