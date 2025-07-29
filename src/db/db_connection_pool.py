import psycopg2
from psycopg2 import pool
import logging
from typing import Optional, Dict, Any

class DBConnectionPool:
    """
    A singleton connection pool for PostgreSQL database connections.
    This helps reduce the overhead of creating and closing connections.
    """
    _instance = None
    _pool = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DBConnectionPool, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 10):
        if self._initialized:
            return
            
        self.db_config = db_config
        self.min_conn = min_conn
        self.max_conn = max_conn
        self._initialized = True
        
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn,
                maxconn=max_conn,
                **db_config
            )
            logging.info(f"Connection pool initialized with {min_conn}-{max_conn} connections")
        except Exception as e:
            logging.error(f"Error initializing connection pool: {e}")
            self._pool = None
    
    def get_connection(self) -> Optional[Any]:
        """Get a connection from the pool"""
        if not self._pool:
            logging.error("Connection pool is not initialized")
            return None
            
        try:
            conn = self._pool.getconn()
            return conn
        except Exception as e:
            logging.error(f"Error getting connection from pool: {e}")
            return None
    
    def release_connection(self, conn: Any) -> None:
        """Return a connection to the pool"""
        if not self._pool:
            logging.error("Connection pool is not initialized")
            return
            
        try:
            self._pool.putconn(conn)
        except Exception as e:
            logging.error(f"Error returning connection to pool: {e}")
    
    def close_all(self) -> None:
        """Close all connections in the pool"""
        if not self._pool:
            return
            
        try:
            self._pool.closeall()
            logging.info("All connections in the pool have been closed")
        except Exception as e:
            logging.error(f"Error closing all connections: {e}")
