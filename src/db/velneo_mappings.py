import psycopg2
from psycopg2 import sql
from datetime import datetime, date
from typing import List, Dict, Optional, Any
import logging
import pytz
from src.db.db_connection_pool import DBConnectionPool




class VelneoMappings:
    """Seleccion de bases de datos"""
    
    def __init__(self, db_config: dict):
        self.config = db_config
        # Initialize the connection pool
        self.pool = DBConnectionPool(db_config, min_conn=2, max_conn=10)
    

    def get_cliente(self):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM clientes 
            WHERE pvsi_clave = 'VTPUB'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving cliente Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_from_general_alm(self):
        """Get the Velneo ID for an almacen (warehouse) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'almacen'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving almacen Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_from_general_serie(self):
        """Get the Velneo ID for a serie from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'serie'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving serie Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_from_general_emp(self):
        """Get the Velneo ID for an empresa (company) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'empresa'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving empresa Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)
    
    def get_from_general_div(self):
        """Get the Velneo ID for an division (company) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'division'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving division Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_metodo_pago(self, reference):
        """Get the Velneo ID for a payment method from metodo_pago table
        
        Args:
            reference: The reference to look for in the pvsi column
            
        Returns:
            int: The velneo value if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM metodo_pago 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving payment method Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_vendedor(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            # Convert reference to string to match the character varying column
            str_reference = str(reference) if reference is not None else None
            
            query = """
            SELECT velneo FROM vendedores 
            WHERE pvsi_clave = %s
            LIMIT 1
            """
            
            cursor.execute(query, (str_reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving vendedor Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_pais(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id FROM pais 
            WHERE description = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving pais ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_tipo_mov(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM tipo_movimiento 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving tipo_movimiento Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_articulo(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo_id FROM articulos 
            WHERE pvsi_clave = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving articulo Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    
    def get_tipo_iva(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM iva 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving tipo_iva Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_from_general_plaza(self):
        """Get the Velneo ID for an plaza (company) from general_misc table
        
        Args:
            reference: The reference to look for (id_psi)
            
        Returns:
            int: The Velneo ID (id_velneo) if found, None otherwise
        """
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id_velneo FROM general_misc 
            WHERE title = 'plaza'
            """
            
            cursor.execute(query)
            result = cursor.fetchone()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"Error retrieving plaza Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_caja_banco(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM caja_banco 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving caja_banco Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_forma_pago(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT velneo FROM forma_pago 
            WHERE pvsi = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving forma_pago Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

    def get_forma_pago_caja_banco(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
           
            # Single query with a fallback to default_value if no match found
            query = """
            SELECT forma_pago FROM forma_pago_caja_banco 
            WHERE caja_banco = %s
            UNION ALL
            SELECT forma_pago FROM forma_pago_caja_banco 
            WHERE caja_banco = 'default_value'
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logging.error(f"Error retrieving forma_pago Velneo ID: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)

            
    def get_fac_id(self, reference):
        conn = None
        cursor = None
        try:
            # Get connection from pool
            conn = self.pool.get_connection()
            if not conn:
                logging.error("Could not get database connection from pool")
                return None
                
            cursor = conn.cursor()
            
            query = """
            SELECT id FROM estado_factura_venta 
            WHERE folio = %s
            LIMIT 1
            """
            
            cursor.execute(query, (reference,))
            result = cursor.fetchone()
            
            return result[0] if result else 0
        
        except Exception as e:
            logging.error(f"Error retrieving get_fac_id Velneo ID: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                # Return connection to pool instead of closing
                self.pool.release_connection(conn)



    