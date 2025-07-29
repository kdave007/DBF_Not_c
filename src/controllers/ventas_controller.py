from datetime import datetime, timedelta, date
import logging
from typing import Dict, Any, List
import json
import time
from ..dbf_enc_reader.core import DBFReader
from ..dbf_enc_reader.connection import DBFConnection
from ..dbf_enc_reader.mapping_manager import MappingManager
from ..config.dbf_config import DBFConfig
import os
import sys

class VentasController:
    def __init__(self, mapping_manager: MappingManager, config: DBFConfig):
        """Initialize the CAT_PROD controller.
        
        Args:
            mapping_manager: Manager for field mappings
            config: DBF configuration
        """
        self.config = config
        self.mapping_manager = mapping_manager
        self.venta_dbf = "VENTA.DBF"  # Header table
        self.partvta_dbf = "PARTVTA.DBF"  # Details table
       
        
        # Initialize DBF reader
        DBFConnection.set_dll_path(self.config.dll_path)
        self.reader = DBFReader(self.config.source_directory, self.config.encryption_password)
    
    def get_sales_in_range(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Get sales data within the specified date range, including details.
        
        Args:
            start_date: Start date for data range
            end_date: End date for data range
            
        Returns:
            List of dictionaries containing the mapped data with nested details
        """
        start_time = time.time()
        
        # First get headers for the date range
        headers_start = time.time()

        headers = self._get_headers_in_range(start_date, end_date)
        headers_time = time.time() - headers_start

        print(f"\nTime to get headers: {headers_time:.2f} seconds")

       
        
        
        # Get folios to filter details
        folios = [str(header['Folio']) for header in headers]
        receipts_num = [{'ref_recibo': str(header['ref_recibo']), 'folio': str(header['Folio'])} for header in headers]

        
        # Then get details only for these folios
        details_start = time.time()

        logging.info(f'/// /// /// Total cabeceras found: {len(headers)}')

        details_by_folio = self._get_details_for_folios(folios) if folios else {}
        receipts_by_ref = self._get_receipts_for_folios(receipts_num, start_date, end_date) if receipts_num else {}
        

        

        details_time = time.time() - details_start
        print(f"Time to get filtered details: {details_time:.2f} seconds")
        
        # Join headers with their details
        join_start = time.time()
        for header in headers:
            folio = header['Folio']  # Using the mapped name from mappings.json
        
            header['recibos'] = receipts_by_ref.get(str(folio), [])
            header['detalles'] = details_by_folio.get(folio, [])
        join_time = time.time() - join_start
        print(f"Time to join headers with details: {join_time:.2f} seconds")
        
        total_time = time.time() - start_time
        print(f"Total processing time: {total_time:.2f} seconds")
        
        return headers
        
    def _get_details_for_folios(self, folios: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Get sales details for specific folios and organize them by folio number.
        
        Args:
            folios: List of folio numbers to get details for
            
        Returns:
            Dictionary mapping folio numbers to lists of detail records
        """
        field_mappings = self.mapping_manager.get_field_mappings(self.partvta_dbf)
        
        # Create filter for specific folios using OR
        filters = []
        for folio in folios:
            # Pad the folio with leading zeros to 6 digits to match DBF format
            filter_dict = {
                'field': 'NO_REFEREN',
                'operator': '=',
                'value': str(folio).zfill(6),  # Pad with leading zeros
                'is_numeric': False  # Treat as string to preserve leading zeros
            }
            filters.append(filter_dict)

        # Get filtered details
        read_start = time.time()

        raw_data_str = self.reader.to_json(self.partvta_dbf, 0, filters)

        read_time = time.time() - read_start
        print(f"Time to read PARTVTA.DBF with filter: {read_time:.2f} seconds")
        
        parse_start = time.time()

        raw_data = json.loads(raw_data_str)
        logging.info(f'/// /// /// Total detalles found: {len(raw_data)}')
        
        
        parse_time = time.time() - parse_start
        print(f"Time to parse PARTVTA JSON: {parse_time:.2f} seconds")

        # Organize details by folio
        details_by_folio = {}
        for record in raw_data:
            print(f' RECORD PRE TRANSFORM {record}')
            transformed = self.transform_record(record, field_mappings)
            if transformed:
                folio = transformed['Folio']  # Using the mapped name
                if folio not in details_by_folio:
                    details_by_folio[folio] = []
                details_by_folio[folio].append(transformed)
        
        return details_by_folio

    def _get_receipts_for_folios(self, reference_records: List[str], start_date: date, end_date: date) -> Dict[str, List[Dict[str, Any]]]:
        """Get sales details for specific reference_records and organize them by folio number.
        
        Args:
            reference_records: List of folio numbers to get details for
            
        Returns:
            Dictionary mapping folio numbers to lists of detail records

        """

        target_table = "FLUJORES.DBF"
        target_table_2 = "FLUJO01.DBF"

        field_mappings = self.mapping_manager.get_field_mappings(target_table)
        field_mappings_2 = self.mapping_manager.get_field_mappings(target_table_2)

        str_start = start_date.strftime("%m-%d-%Y")
        str_end = end_date.strftime("%m-%d-%Y")
        
        # Create filter for specific reference_records using OR
        filters = []
        
        # for ref_rec in reference_records:
            # Pad the folio with leading zeros to 6 digits to match DBF format
        filter_dict = {
            'field': 'FECHA',
            'operator': 'range',
            'from_value': str_start,  # Format to match DBF M/D/Y
            'to_value':str_end,  # End of day
            'is_date': False  # F_
        }
        filters.append(filter_dict)

        # Get filtered details
        read_start = time.time()

        raw_data_str = self.reader.to_json(target_table, 0, filters)
        raw_data_2_str = self.reader.to_json(target_table_2, 0, filters)

        read_time = time.time() - read_start
        print(f"Time to read tables with filter: {read_time:.2f} seconds")
        
        parse_start = time.time()

        # Parse both JSON strings
        raw_data_1 = json.loads(raw_data_str)
        raw_data_2 = json.loads(raw_data_2_str)
        
        # Combine the data from both tables
        raw_data = raw_data_1 + raw_data_2

        logging.info(f'/// /// /// Total recibos found: {len(raw_data)}')
        
        print(f"Records from {target_table}: {len(raw_data_1)}")
        print(f"Records from {target_table_2}: {len(raw_data_2)}")
        print(f"Total combined records: {len(raw_data)}")
        

      

        parse_time = time.time() - parse_start
        print(f"Time to parse {target_table} JSON: {parse_time:.2f} seconds")
        # print(raw_data_str)

        # Create a dictionary to store matched receipts by folio
        receipts_by_folio = {}
        
        # Process each reference record
        for ref in reference_records:
            ref_recibo = ref.get('ref_recibo')
            folio = ref.get('folio')
            
            if ref_recibo and folio:
                # Initialize an empty list for this folio if it doesn't exist
                if folio not in receipts_by_folio:
                    receipts_by_folio[folio] = []
                
                # Find all matching records in raw_data where REF_NUM equals ref_recibo
                for record in raw_data:
                    if 'REF_NUM' in record and str(record['REF_NUM']) == str(ref_recibo):
                        # Transform the record and add it to the list for this folio
                        transformed = self.transform_record(record, field_mappings)
                        if transformed:
                            receipts_by_folio[folio].append(transformed)

        # print(f' receipts {receipts_by_folio}')
        
        return receipts_by_folio
        
    def _get_headers_in_range(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Get sales headers within the specified date range."""
        field_mappings = self.mapping_manager.get_field_mappings(self.venta_dbf)
        str_start = start_date.strftime("%m-%d-%Y")
        str_end = end_date.strftime("%m-%d-%Y")
        
        # Create a single filter for the date range
        filters = [{
            'field': 'F_EMISION',
            'operator': 'range',
            'from_value': str_start,  # Format to match DBF M/D/Y
            'to_value':str_end,  # End of day
            'is_date': False  # F_EMISION is stored as string
        }]
        print(f"\nSearching for date range: {start_date} to {end_date}")
        
        read_start = time.time()
        raw_data_str = self.reader.to_json(self.venta_dbf, self.config.limit_rows, filters)
        read_time = time.time() - read_start
        print(f"Time to read VENTA.DBF: {read_time:.2f} seconds")
        
        parse_start = time.time()
        raw_data = json.loads(raw_data_str)
        parse_time = time.time() - parse_start
        print(f"Time to parse VENTA JSON: {parse_time:.2f} seconds")

        transformed_data = []
        for record in raw_data:
           
            if record.get('TIPO_DOC') == 'DV':#only add DV records
              
                transformed = self.transform_record(record, field_mappings)
                if transformed:
                    transformed_data.append(transformed)
        # print(f' {transformed_data}')
        # sys.exit()            
        
        return transformed_data

    def sanitize_string(self, text):
        """
        Sanitize a string value to prevent issues with special characters.
        
        Args:
            text: The string to sanitize
            
        Returns:
            Sanitized string
        """
        if not isinstance(text, str):
            return text
            
        # Replace problematic characters or escape them
        # This handles quotes and other special characters
        return text.replace('"', '\"').replace("'", "\'")
    
    def transform_record(self, record: Dict[str, Any], field_mappings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a DBF record using the field mappings.
        
        Args:
            record: Raw record from DBF
            field_mappings: Field mapping configuration
            
        Returns:
            Transformed record with mapped field names and types
        """
        transformed = {}
        for target_field, mapping in field_mappings.items():
            dbf_field = mapping['dbf']
            if dbf_field in record:
                value = record[dbf_field]
                
                # Sanitize string values to handle special characters
                if isinstance(value, str):
                    value = self.sanitize_string(value)
                    
                if mapping['type'] == 'number':
                    try:
                        # Check if this is a reference number that needs to preserve leading zeros
                        if dbf_field.startswith('NO_REFEREN') or dbf_field.startswith('NUMERO_A') :
                            # Keep it as string to preserve leading zeros
                            pass
                        else:
                            value = float(value) if '.' in str(value) else int(value)
                    except (ValueError, TypeError):
                        value = 0
                
                transformed[mapping['velneo_table']] = value
                
        # Print first record for debugging
        if not hasattr(VentasController, '_printed_transform'):
            print("\nTransformed record example:", transformed)
            setattr(VentasController, '_printed_transform', True)
                
        return transformed
