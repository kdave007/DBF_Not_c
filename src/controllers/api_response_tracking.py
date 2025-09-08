import os
import sys
from datetime import datetime, date
from src.config.db_config import PostgresConnection
from src.db.response_tracking import ResponseTracking
from src.db.detail_tracking import DetailTracking
from src.db.receipt_tracking import ReceiptTracking


class APIResponseTracking:
    def __init__(self):
        self.db_config = PostgresConnection.get_db_config()
        
        # Initialize ResponseTracking with the configuration dictionary
        self.resp_tracking = ResponseTracking(self.db_config)

        self.resp_detail_tracking = DetailTracking(self.db_config)
        self.resp_receipt_tracking = ReceiptTracking(self.db_config)

    def _create_op(self, item):
        action = item.get('accion')
        estado = item.get('estado')
        waiting_id = item.get('id')
          
        # Parse the date string from DBF format to a proper date object
        print(f'item to insert {item}')
        
        fecha_str = item.get('fecha_emision')
        create_index = 0 #since it does not have an assigned index by server yet, set it as 0
        doc_type = "DV"

        try:
            # Remove the 'a. m.' or 'p. m.' part and parse the date
            fecha_str = fecha_str.replace(' a. m.', '').replace(' p. m.', '')
            # Format is day/month/year in the DBF records
            fecha_date = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S').date()
        except (ValueError, AttributeError):
            # Fallback to current date if parsing fails
            fecha_date = datetime.now().date()
            print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
        
        return self.resp_tracking.insert_fac(
            create_index,
            item.get('folio'),
            item.get('total_partidas'),
            item.get('hash'),
            estado,
            action,
            fecha_date,
            item.get('total_recibos'),
            waiting_id,
            doc_type
        )
   
    def _details_waiting(self, records):
        """
        Process completed details (partidas) from API response
        """

        details = records.get('partidas')
        action = records.get('accion')
        estado = records.get('estado')

        if details:
            return self.resp_detail_tracking.insert_details_on_wait(details, action, estado)
        return False

    def _receipts_waiting(self, records):
        """
        Process completed details (partidas) from API response
        """

        receipts = records.get('recibos')
        action = records.get('accion')
        estado = records.get('estado')

        if receipts:
            return self.resp_receipt_tracking.insert_receipts_on_wait(receipts, action, estado)
        return False

    
    def _receipts_completed(self, records):
        """
        Process completed receipts (recibos) from API response
        """
        receipts = records.get('recibos')
        if receipts:
            return self.resp_receipt_tracking(receipts)
        return False
            

    def _update_op(self, results):
        action = 'modificado'
        estado = 'ca_completado'
        done = False
        execute = False

        if results.get('success'):
            execute = True
            for item in results.get('success'):
              
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                   # Extract only the date part (DD/MM/YYYY) and ignore time
                    if fecha_str and isinstance(fecha_str, str):
                        # Get only the date part by splitting on space and taking first part
                        date_part = fecha_str.split(' ')[0]
                        # Split by / to get day, month, year
                        day, month, year = date_part.split('/')
                        # Create date object with just the date components
                        
                        fecha_date = date(int(year), int(month), int(day))
                    else:
                        fecha_date = datetime.now().date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                done = self.resp_tracking.insert_fac(
                    item.get('id'),
                    item.get('folio'),
                    item.get('total_partidas'),
                    item.get('hash'),
                    estado,
                    action,
                    fecha_date
                )

        return {'done': done, 'execute':execute}


    def _delete_op(self, results):
        action = 'eliminado'
        estado = 'ca_eliminado'
        done = False
        execute = False

        if results.get('success'):
           
            execute = True
            for item in results.get('success'):
                print(f" ITEM {item}")
                # Parse the date string from DBF format to a proper date object
                fecha_str = item.get('fecha_emision')
                try:
                    # Remove the 'a. m.' or 'p. m.' part and parse the date
                    fecha_str = fecha_str.replace(' a. m.', '').replace(' p. m.', '')
                    # Format is day/month/year in the DBF records
                    fecha_date = datetime.strptime(fecha_str, '%d/%m/%Y %H:%M:%S').date()
                except (ValueError, AttributeError):
                    # Fallback to current date if parsing fails
                    fecha_date = datetime.now().date()
                    print(f"Warning: Could not parse date '{fecha_str}', using current date instead")
                
                done = self.resp_tracking.delete_by_id(
                    item.get('id')
                )
                
    def _head_completed(self, record):
        """Update record status to indicate that all details have been processed
        
        Args:
            id: The ID of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
        """

        estado = record.get('estado')
        action = record.get('accion')
        folio = record.get('folio')
        new_id = record.get('id')
        tipo_doc = 'DV'
        
        print(f"Updating record {id} to status: {estado}, action: {action}")
        
        return self.resp_tracking.update_head_status(folio, new_id, estado, action, tipo_doc)

    def _detail_completed(self, records):
        """Update record status to indicate that all details have been processed
        
        Args:
            id: The ID of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
        """
  
        details = records.get('partidas')    
        
        return self.resp_tracking.update_detail_status(details)

    def _receipt_completed(self, records):
        """Update record status to indicate that all details have been processed
        
        Args:
            id: The ID of the record to update
            
        Returns:
            bool: True if the update was successful, False otherwise
        """
        
        receipts = records.get('recibos')   
        
        return self.resp_tracking.update_receipt_status(receipts)


    def update_create_details(self, records):
        """
        Insert or update records in the database
        
        Args:
            db_connection: Database configuration dictionary
            records: List of records to insert/update
            
        Returns:
            Number of records successfully processed
        """
        if not records:
            return 0
            
        # Create a DetailTracking instance with the database configuration
        # db_connection is now a dictionary, not a PostgresConnection object
        detail_tracker = DetailTracking(self.db_config)
        
        # Use batch_insert_details method to insert all records at once
        success = detail_tracker.batch_replace_by_id(records)
        
        if success:
            return len(records)
        else:
            print("Error inserting records")
            return 0