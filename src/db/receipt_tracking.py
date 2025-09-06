
import psycopg2
import logging
import json
from typing import List, Dict
from datetime import date, datetime

class ReceiptTracking:
    
    def __init__(self, db_config: dict):
        self.config = db_config

    def insert_receipts_on_wait(self, receipts: List[Dict], action, estado) -> bool:
        try:
            # Connect to database
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                inserted_count = 0
                
                # Simple insert query
                insert_query = """
                    INSERT INTO recibo_venta (
                        folio, num_ref, dtl_cob_apl_t, dtl_doc_cob_t, cta_cor_t, rbo_cob_t, 
                        hash, estado, fecha_emision, fecha_procesamiento, indice
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP,  %s)
                """
                print(f'RECEIPTS TO PROCESS: {receipts}')

                # Process each receipt
                with conn.cursor() as cursor:
                    for receipt in receipts:
                        # Get basic fields from the receipt
                        num_ref = receipt.get('num_ref', '')
                        folio = receipt.get('folio', '')
                        indice = receipt.get('indice')
                        
                        # Get ID fields directly from the receipt
                        dtl_cob_apl_t = receipt.get('id_dtl_cob_apl_t')
                        dtl_doc_cob_t = receipt.get('id_dtl_doc_cob_t')
                        cta_cor_t = receipt.get('id_cta_cor_t')
                        rbo_cob_t = receipt.get('id_rbo_cob_t')
                        
                        # Get fecha_emision (default to today)
                        fecha_emision = receipt.get('fecha_emision', date.today())
                        if isinstance(fecha_emision, str):
                            try:
                                # Try to parse date in format '15/07/2025 12:00:00 a. m.'
                                fecha_emision = datetime.strptime(fecha_emision.split(' ')[0], '%d/%m/%Y').date()
                            except ValueError:
                                try:
                                    # Try standard ISO format
                                    fecha_emision = datetime.strptime(fecha_emision, '%Y-%m-%d').date()
                                except ValueError:
                                    # Default to today if parsing fails
                                    fecha_emision = date.today()
                        elif isinstance(fecha_emision, datetime):
                            fecha_emision = fecha_emision.date()
                        
                        # Get hash and estado (default values if not provided)
                        hash_value = receipt.get('hash', '')

                        try:
                            # Insert the record
                            cursor.execute(insert_query, (
                                folio, num_ref, dtl_cob_apl_t, dtl_doc_cob_t, cta_cor_t, rbo_cob_t,
                                hash_value, estado, fecha_emision, indice
                            ))
                            inserted_count += 1
                            
                        except Exception as e:
                            print(f"RECEIPTS Error inserting record: {e}")
                            logging.error(f" RECEIPTS :: Error inserting record: {e}")
                            # Continue with next record

                        logging.info(f"Inserting record: folio={folio}, num_ref={num_ref}, dtl_cob_apl_t={dtl_cob_apl_t}")


                # Commit all changes
                conn.commit()
                print(f"Batch replace completed: {inserted_count} records inserted")
                return inserted_count > 0
        
        except Exception as e:
            logging.error(f" RECEIPTS :: Error in batch_replace_by_id: {e}")
            print(f"Database connection error: {e}")
            return False
        
    def batch_replace_by_id(self, receipts: List[Dict]) -> bool:
        """Procesa múltiples recibos en una sola transacción, utilizando el ID como referencia principal."""
        if not receipts:
            return True  # Nothing to process
            
        try:
            # Connect to database
            with psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            ) as conn:
                inserted_count = 0
                
                # Simple insert query
                insert_query = """
                    INSERT INTO recibo_venta (
                        folio, num_ref, dtl_cob_apl_t, dtl_doc_cob_t, cta_cor_t, rbo_cob_t, 
                        hash, estado, fecha_emision, fecha_procesamiento
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                print(f'RECEIPTS TO PROCESS: {receipts}')
                
                # Process each receipt
                with conn.cursor() as cursor:
                    for receipt in receipts:
                        # Get basic fields from the receipt
                        num_ref = receipt.get('num_ref', '')
                        folio = receipt.get('folio', '')
                        
                        # Get ID fields directly from the receipt
                        dtl_cob_apl_t = receipt.get('id_dtl_cob_apl_t')
                        dtl_doc_cob_t = receipt.get('id_dtl_doc_cob_t')
                        cta_cor_t = receipt.get('id_cta_cor_t')
                        rbo_cob_t = receipt.get('id_rbo_cob_t')
                        
                        # Get fecha_emision (default to today)
                        fecha_emision = receipt.get('fecha_emision', date.today())
                        if isinstance(fecha_emision, str):
                            try:
                                # Try to parse date in format '15/07/2025 12:00:00 a. m.'
                                fecha_emision = datetime.strptime(fecha_emision.split(' ')[0], '%d/%m/%Y').date()
                            except ValueError:
                                try:
                                    # Try standard ISO format
                                    fecha_emision = datetime.strptime(fecha_emision, '%Y-%m-%d').date()
                                except ValueError:
                                    # Default to today if parsing fails
                                    fecha_emision = date.today()
                        elif isinstance(fecha_emision, datetime):
                            fecha_emision = fecha_emision.date()
                        
                        # Get hash and estado (default values if not provided)
                        hash_value = receipt.get('hash', '')
                       

                        logging.info(f"Inserting record: folio={folio}, num_ref={num_ref}, dtl_cob_apl_t={dtl_cob_apl_t}")
                        
                        try:
                            # Insert the record
                            cursor.execute(insert_query, (
                                folio, num_ref, dtl_cob_apl_t, dtl_doc_cob_t, cta_cor_t, rbo_cob_t,
                                hash_value, estado, fecha_emision
                            ))
                            inserted_count += 1
                            
                        except Exception as e:
                            print(f"RECEIPTS Error inserting record: {e}")
                            logging.error(f" RECEIPTS :: Error inserting record: {e}")
                            # Continue with next record
                
                # Commit all changes
                conn.commit()
                print(f"Batch replace completed: {inserted_count} records inserted")
                return inserted_count > 0
                
        except Exception as e:
            logging.error(f" RECEIPTS :: Error in batch_replace_by_id: {e}")
            print(f"Database connection error: {e}")
            return False