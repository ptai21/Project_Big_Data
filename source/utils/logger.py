import logging
import sys
import os

def get_logger(name):
    # Tạo thư mục logs nếu chưa có
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s')

        # 1. Ghi ra Console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. Ghi ra File
        file_handler = logging.FileHandler('logs/etl_process.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger