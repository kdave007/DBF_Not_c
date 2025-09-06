import psycopg2
import logging
import json
from typing import List, Dict
from datetime import date, datetime

class PendingServerRecords:
    
    def __init__(self, db_config: dict):
        self.config = db_config
    
    def get_pending_records(self, limit=100):
        """
        Get records with 'pendiente' status from estado_factura_venta table
        
        Args:
            limit (int): Maximum number of records to retrieve
            
        Returns:
            list: List of pending records or empty list if none found
        """
        try:
            # Connect to database
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                
                # Correct SQL query with FROM clause and proper WHERE syntax
                query = """
                    SELECT id, folio, fecha_emision, hash, fecha_procesamiento , total_partidas, total_recibos
                    FROM estado_factura_venta
                    WHERE estado = 'pendiente' AND accion = 'enviado'
                    ORDER BY fecha_procesamiento DESC
                    LIMIT %s
                """
                
                with conn.cursor() as cursor:
                    # Execute the query with the limit parameter
                    cursor.execute(query, (limit,))
                    
                    # Fetch all results
                    rows = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    result = []
                    for row in rows:
                        result.append({
                            'id': row[0],
                            'folio': row[1],
                            'fecha_emision': row[2],
                            'hash': row[3],
                            'fecha_procesamiento': row[4],
                            'total_partidas': row[5],
                            'total_recibos': row[6]
                        })
                    
                    if not result:
                        logging.info("No pending records found in estado_factura_venta")
                    else:
                        logging.info(f"Found {len(result)} pending records")
                        
                    return result
                    
        except Exception as e:
            logging.error(f"pending_server_records :: Error select query: {e}")
            print(f"Database connection error: {e}")
            return False