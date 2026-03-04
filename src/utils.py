
# src/utils.py
import logging
import yaml
import sys
import os

def get_logger(name="DataCorePipeline"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # --- ĐOẠN SỬA ĐỔI ---
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir) # Tự động tạo thư mục logs nếu chưa có
        # --------------------

        # Ghi ra console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Ghi ra file
        file_handler = logging.FileHandler(os.path.join(log_dir, "pipeline.log"), encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def load_config(config_path="config.yaml"):
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)