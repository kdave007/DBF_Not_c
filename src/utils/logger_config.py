import logging
import os
from datetime import datetime
from src.utils.get_enc import EncEnv

def setup_logging(log_level=logging.INFO):
    """
    Configure logging to output to both console and file.
    
    Args:
        log_level: The logging level (default: logging.INFO)
    """
    # Load environment variables if not already loaded
    from dotenv import load_dotenv
    load_dotenv()
    env = EncEnv()
    
    # Get log directory from environment variable or use default
    default_logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    logs_dir = env.get('LOG_PATH', default_logs_dir)
    
    # Create logs directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename with date only (one file per day)
    date_today = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(logs_dir, f'dbf_recibos_{date_today}.log')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    # Add handlers to root logger
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    
    return log_file
