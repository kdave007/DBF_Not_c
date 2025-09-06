import logging
from src.db.pending_server_records import PendingServerRecords
from src.controllers.send_request import SendRequest
from src.db.velneo_mappings import VelneoMappings
from src.utils.get_enc import EncEnv
import sys
    


class PendingRecordsController:
    """
    Controller for handling pending records that need to be checked with the server
    """
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.pending_records = PendingServerRecords(db_config)
        self.env = EncEnv()
        self.velneo_mappings = VelneoMappings(db_config)
        self.class_name = self.__class__.__name__
        
    def get_pending_records(self, limit=100):
        """
        Get records with pending status from the database
        
        Args:
            limit (int): Maximum number of records to retrieve
            
        Returns:
            list: List of pending records
        """
        results = []
        pending = self.pending_records.get_pending_records(limit)
        # print(f'PENDING LIST : {pending}')
        
        results = self._format_records(pending)
        # print(f'PENDING LIST : {results}')
      
        return results

    
    def _format_records(self, records):
        """
        Format records for delivery check, adding the serie from velneo mappings
        
        Args:
            records (list): List of records from the database
            
        Returns:
            list: Formatted records with serie added
        """
        if not records or records == 0:
            return []
            
        formatted_records = []
        
        
        for record in records:
            # Get the store from the folio (assuming folio format contains store info)
            formatted_record = {}
            formatted_record['num_doc'] = record.get('folio')
            formatted_record['id'] = int(record.get('id'))

            #mainly used for debug simulated response mode
            formatted_record['total_partidas'] = int(record.get('total_partidas', 0))
            formatted_record['total_recibos'] = int(record.get('total_recibos', 0))
       
            # Extract store from folio or use a default
            store = self.env.get("CLAVE_SUCURSAL")

            # Format the date as YYYY-MM-DD
            fecha_emision = record.get('fecha_emision')
            formatted_record['fecha'] = fecha_emision.strftime('%Y-%m-%d') if hasattr(fecha_emision, 'strftime') else None
            
            # Get serie from velneo mappings
            formatted_record['serie'] = self.velneo_mappings.get_from_general_serie(store)

            # print(f' ******************FORMATTING {formatted_record}')
            formatted_records.append(formatted_record)
            
        return formatted_records
    
   