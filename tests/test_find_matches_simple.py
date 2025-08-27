import os
import sys
from pathlib import Path
from datetime import date
import logging
from dotenv import load_dotenv

# Configurar correctamente el PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Now we can initialize logging
from src.utils.logger_config import setup_logging
log_file = setup_logging()
# print(f"Logs will be saved to: {log_file}")

# Use logging instead of print for the decorative message
border = "*" * 80
spacing = "*" + " " * 78 + "*"
message = "*" + " " * 25 + "STARTING SCRIPT EXECUTION v 1.2" + " " * 25 + "*"

logging.info(border)
logging.info(spacing)
logging.info(message)
logging.info(spacing)
logging.info(border)

# Now we can import from src
from src.config.dbf_config import DBFConfig

print(f"PYTHONPATH: {sys.path}")  # Debug

try:
    from src.controllers.main_workflow import WorkFlow
except ImportError as e:
    print(f"ImportError: {e}")
    raise

def main():
    print("=== Starting simple test for MatchesProcess ===")
       # Let's try with the exact date from your screenshot: 20/03/2025
    # start_date = date(2025, 5, 5)  # year month day
    # end_date = date(2025, 5, 5)  # year month day

    # Check if script should be stopped based on encrypted .env flag
    from src.utils.get_enc import EncEnv
    env = EncEnv()
    
    stop_script = env.get('STOP_SCRIPT', 'False') == 'True'
    logging.info(f" STOP_SCRIPT : {env.get('STOP_SCRIPT', 'False')} ")
    logging.info(f" DEBUG_MODE : {env.get('DEBUG_MODE', 'False')} ")
    logging.info(f" SQL_ENABLED : {env.get('SQL_ENABLED', 'False')} ")
   
    #internet validation

    # Check internet connection if required by environment variable
    internet_check = env.get('INTERNET_CHECK', 'True').lower() == 'true'
    if internet_check:
        from src.utils.network_utils import check_internet_connection
        internet_available, error_message = check_internet_connection()
        if not internet_available:
            message = f"Internet connection check failed: {error_message}m ending the process"
            print(message)
            logging.error(message)
            sys.exit(1)
        else:
            print("Internet connection verified successfully")
    
    if stop_script:
        message = "STOP_SCRIPT flag is set to True in .env - Exiting script early"
        print(message)
        logging.warning(message)
        sys.exit(0)
    
    # Import and use DateManager to get dates
    from src.utils.date_manager import DateManager
    date_manager = DateManager()
    start_date, end_date = date_manager.get_dates()
    
    print(f"Start date: {start_date} - {type(start_date)}")
    print(f"End date: {end_date} - {type(end_date)}")
    
    # Exit after printing dates
    # sys.exit(0)
    
    #Original code (won't be executed due to sys.exit above)
    # start_date = date(2025, 7, 1)  # year month day
    # end_date = date(2025, 7, 2)  # year month day

    logging.info(f'start date : {start_date} to end_date {end_date}')
    
    process = WorkFlow()

    # url_source_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Recibos\pospcp"
    # url_dll_A = r"C:\Users\gtdri\Documents\projects\care\DBF_Bridge\Advantage.Data.Provider.dll"

    # url_dll_B=r"C:\Users\campo\Documents\projects\DBF_Bridge\Advantage.Data.Provider.dll"
    # url_source_B=r"C:\Users\campo\Documents\projects\DBF_Recibos\pospcp"

    # config = DBFConfig(
    #     dll_path=url_dll_B,
    #         encryption_password="X3WGTXG5QJZ6K9ZC4VO2",
    #         source_directory=url_source_B,
    #         limit_rows=500  # Limit to 3 sales for testing
    # )
    

    # Use environment variables from .env file instead of hardcoded values
    try:
        # DBFConfig will automatically load values from .env file
        config = DBFConfig(
            # No need to specify these values as they'll be loaded from .env
            # dll_path, encryption_password, and source_directory will be loaded from .env
            limit_rows=10000  # Limit to 500 sales for testing
        )
        result = process.start(config, start_date, end_date)
        if result:
            print("Test completed successfully!")
        else:
            print("Test completed with warnings")
        return result
    except Exception as e:
        print(f"Test failed with error: {str(e)}")
        return False

if __name__ == "__main__":
    main()
