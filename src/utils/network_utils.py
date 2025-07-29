"""
Network utilities for checking internet connectivity.
"""
import socket
import logging
from typing import Tuple, Optional

def check_internet_connection(host: str = "8.8.8.8", port: int = 53, timeout: int = 3) -> Tuple[bool, Optional[str]]:
    """
    Check if there is an active internet connection by attempting to connect to a reliable host.
    
    Args:
        host: The host to connect to (default: Google's DNS server)
        port: The port to connect to (default: 53 for DNS)
        timeout: Connection timeout in seconds
        
    Returns:
        Tuple containing:
        - Boolean indicating if internet is available
        - Error message if connection failed, None otherwise
    """
    try:
        # Attempt to create a socket connection to the specified host and port
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        logging.info("Internet connection test: SUCCESSFUL")
        return True, None
    except socket.error as ex:
        error_message = f"Internet connection test: FAILED - {str(ex)}"
        logging.warning(error_message)
        return False, error_message
