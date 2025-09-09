from ast import Try
import os
import sys
from turtle import st
from pathlib import Path
from src.config.db_config import PostgresConnection
from src.db.response_tracking import ResponseTracking
from src.utils.response_simulator import ResponseSimulator
import requests
import json
import logging
from decimal import Decimal
from datetime import datetime, date
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv
# Load environment variables
load_dotenv()

# Set debug flag from .env - set to True to use simulated responses instead of real API calls
env = EncEnv()
DEBUG_MODE = env.get('DEBUG_MODE', 'True').lower() == 'true'
CLIENT_ID = env.get('CLIENT_ID')

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

class SendRequest:

    def __init__(self):
        # Get database configuration as a dictionary
        self.db_config = PostgresConnection.get_db_config()
        # Initialize ResponseTracking with the configuration dictionary
        self.response_tracking = ResponseTracking(self.db_config)
        


    def waiting_line(self, record, base_url, api_key):
        """
        Process a single record and send it to the API
        
        Args:
            record: A single record dictionary containing 'folio' and 'dbf_record'
            
        Returns:
            Dictionary with 'success' and 'failed' lists containing the operation result
        """
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }

        headers = {
            "Content-Type": "application/json",
         
             "accept": "*/*",
  
            "accept-encoding": "gzip, deflate, br",
        }
        
        folio = record.get('folio')
        dbf_record = record.get('dbf_record', {})

        
        # Add decorative logging for sending folio
        border = "=" * 80

        if len(dbf_record.get('detalles', [])) == 0 :
            logging.warning(f"Declined send request for folio {folio} found with {len(dbf_record.get('detalles', []))} detalles and {len(dbf_record.get('recibos', []))} recibos")
            results['failed'].append({
                        'folio': folio,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'hash': "",
                        'status': 500,
                        'error_msg': "Skipped due to empty recibos or detalles"
                    })
            
            return results
            
        
        logging.info(f"SENDING REQUEST FOR FOLIO: {folio}, det:{len(dbf_record.get('detalles', []))} , rec:{len(dbf_record.get('recibos', []))}")
       
        
        try:
            # Prepare payload for the single record
            try:
                formatted_details = self._format_details(dbf_record)
                formatted_receipts = self._format_receipts(dbf_record)
                # Prepare payload for a single record
                single_payload = {
                    "emp": str(dbf_record.get('emp')),
                    "emp_div": str(dbf_record.get('emp_div')),
                    "num_doc": folio,
                    # "clt": dbf_record.get('clt'),
                    "clt": int(CLIENT_ID),
                    "fpg": dbf_record.get('fpg'),
                    # "fpg": 20,
                    "cmr": dbf_record.get('cmr'),
                    "fch": self._format_date_to_iso(dbf_record.get("fecha")),
                    # "tot_fac": dbf_record.get("total_bruto"),
                    "ser": dbf_record.get('ser'),
                    "hor": dbf_record.get('hor'),
                    "pai": dbf_record.get('pai'),
                    "ent_rel_tip": 1,
                    "mon_c": 1,
                    "cot": 1,
                    "fch_vto": self._format_date_to_iso(dbf_record.get("fecha")),
                    "pre_con_iva_inc": 0,
                    "trm": 1,
                    "dum": 1,
                    "alm": str(dbf_record.get('alm')),
                    "fac": "1",
                    "off": 1,
                    "detalles": formatted_details ,
                    "recibos": formatted_receipts,
                    "usr":1,
                    "aut_usr":1,
                    "usr":1,
                    "por_dto":0,
                    "vta_fac_g": dbf_record.get('vta_fac_g'),## THIS IS THE FAC ID RELATED
                    "num_det":len(formatted_details),
                    "num_rec":len(formatted_receipts)
                }
            except Exception as e:
                print(f'Error preparing payload: {e}')
                raise
            
            print(f' OG RECORD AS : {dbf_record}')
            # Send the record
            print(f"Sending record for folio {folio}")
            post_data = json.dumps(single_payload, cls=CustomJSONEncoder, indent=4)
            print(f"POST Request URL: {base_url}?api_key={api_key}")
            print(f"POST Request Data:\n{post_data}")
            
            # Use simulated response if DEBUG_MODE is enabled
            if DEBUG_MODE:
                print(f"DEBUG MODE: Using simulated response for folio {folio}")
                # status_code, response_json = ResponseSimulator.simulate_response(dbf_record, folio)
                # response = ResponseSimulator.create_mock_response(status_code, response_json)
                response = ResponseSimulator.simulate_id_response()
            else:
                # Make actual API request
                response = requests.post(
                    f"{base_url}?api_key={api_key}", 
                    headers=headers,
                    data=post_data,
                    timeout=60  # Set timeout to 60 seconds
                )
            
            print(f"Response Status Code for folio {folio}: {response.status_code}")
            print(f"Response Headers for folio {folio}: {response.headers}")

            logging.info(f"Response Status Code for folio {folio}: {response.status_code}")
            
            # Process the response
            response_value = response.text
         
            if response.status_code in [200, 201, 202, 204] and response_value is not 0:
                
                # formatted_json = json.dumps(response_json, indent=4, sort_keys=False)
                print(f"Response waiting line ID for folio {folio}: {response_value}")

                success_entry = {
                        'folio': folio,
                        'id': response_value,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'total_recibos': len(dbf_record.get('recibos', [])),
                        'hash': dbf_record.get('md5_hash', ''),
                        'status': response.status_code,
                        'accion':'enviado',
                        'estado':'pendiente',
                        'partidas': [],
                        'recibos': []
                }

                if len(dbf_record.get('detalles', [])) > 0 :
                    for index, detail in enumerate(dbf_record.get('detalles', []),1):
                        data = {
                            'id': 0,
                            'indice': index,
                            'folio': str(folio),
                            'ref': detail['REF'],
                            'fecha': self._format_date_to_iso(dbf_record.get('fecha')),
                            'detail_hash':detail['detail_hash'],
                        }

                        success_entry['partidas'].append(data)
                                

                if len(formatted_receipts) > 0 :
                    for index, rec in enumerate(dbf_record.get('recibos', []),1):
                        data = {
                            'id_dtl_cob_apl_t': None,
                            'id_cta_cor_t': None,
                            'id_dtl_doc_cob_t': None,
                            'id_rbo_cob_t': None,
                            'id_fac': 0,
                            'indice':index,
                            'accion':'enviado',
                            'estado':'pendiente',
                            'num_ref':rec.get('ref_recibo'),
                            'folio': str(folio),  # From CA
                            'fecha':self._format_date_to_iso(dbf_record.get('fecha'))
                        }

                        success_entry['recibos'].append(data)
                
                results['success'].append(success_entry)
                print(f"Successfully processed response for folio {folio}")
                    
            else:
                results['failed'].append({
                    'folio': folio,
                    'fecha_emision': dbf_record.get('fecha'),
                    'total_partidas': len(dbf_record.get('detalles', [])),
                    'total_recibos': len(dbf_record.get('recibos', [])),
                    'hash': dbf_record.get('dbf_hash', ''),
                    'status': response.status_code,
                    'error_msg': "response by server : "+response_value
                })                    
                

        #     
        except Exception as e:
            logging.info(f"Exception during create operation: {str(e)}")
            error_message = f"Exception during create operation: {str(e)}"
            print(error_message)
            # Mark the record as failed
            results['failed'].append({
                'folio': folio, 
                'fecha_emision': dbf_record.get('fecha'),
                'hash': record.get('dbf_hash', ''),
                'status': None,
                'error_msg': error_message
            })
                  
        return results
            
    

        
    def _format_date_to_iso(self, date_str):
        """
        Convert date from format like "30/04/2025 12:00:00 a. m." to "2025-04-30"
        
        Args:
            date_str: Date string in DD/MM/YYYY format with possible time component
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        if not date_str:
            return ""
            
        try:
            # Split by space to separate date and time
            parts = date_str.split(' ')
            date_part = parts[0]
            
            # Split the date part by /
            day, month, year = date_part.split('/')
            
            # Format to YYYY-MM-DD
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except Exception as e:
            print(f"Error formatting date {date_str}: {e}")
            return date_str  # Return original if parsing fails
            
    def _format_hour_to_12h(self, hour_value):
        """
        Format hour value with minutes and seconds
        
        Args:
            hour_value: Integer representing hour in 24-hour format (0-23)
            
        Returns:
            String in format "hh:00:00"
        """
        if hour_value is None:
            return ""
            
        try:
            # Convert to integer if it's a string
            if isinstance(hour_value, str):
                hour_value = int(hour_value)
                
            # Format hour with minutes and seconds
            return f"{hour_value:02d}:00:00"
        except Exception as e:
            print(f"Error formatting hour: {e}")
            return f"{hour_value}:00:00"  # Return original if parsing fails
            
    def _format_details(self, parent_ref):
        records = parent_ref.get('detalles')
        array_payload = []
        for index, record in enumerate(records,1):
            single_payload = {
                        "_indice":index,
                        "alm":str(record.get('alm')),
                        "art": record.get('art'),
                        "und_med":1,
                        "can_und": record.get('cantidad'),
                        "can":record.get('cantidad'),
                        "emp_div": str(record.get('emp_div')),
                        "emp": str(record.get('emp')),
                        "fch": self._format_date_to_iso(parent_ref.get("fecha")),
                        "hor":record.get('hor'),
                        # "pre": float(record.get('precio', 0)) ,
                        "pre": float(record.get('precio', 0)) + float(record.get('n_descto_1', 0)) + float(record.get('n_descto_2', 0)) ,
                        # "pre": float(record.get('imp_part', 0)) + float(record.get('iva_part', 0)),
                        "por_dto": record.get('descuento'),
                        "reg_iva_vta":record.get('reg_iva_vta'),
                        # "vta_fac": parent_ref.get('parent_id'),
                        "clt":record.get('clt'),
                        "mov_tip":"C",
                        "cal_arr":1,
                        "desc":record.get('desc_adi')
                    }
            array_payload.append(single_payload)
        return array_payload

    def _format_receipts(self, parent_ref):
        records = parent_ref.get('recibos')
        array_payload = []
     
        for index, record in enumerate(records,1):
            single_payload = {
                        "_indice":index,
                        "ser":3,
                        "fch": self._format_date_to_iso(parent_ref.get("fecha")),
                        "ref_recibo": record.get('ref_recibo'),
                        "importe": record.get('importe'),
                        "caja_bco": record.get('caja_bco'),
                        "tienda": record.get('tienda'),
                        "ref_tipo": record.get('ref_tipo'),
                        "hora": record.get('hora'),
                        "num_doc": f'{record.get('plaza')}-{record.get('tienda')}-{record.get('ref_tipo')}-{record.get('ref_recibo')}',
                        "fpg": record.get('fpg')
                    }
            print(record)        
            array_payload.append(single_payload)
        return array_payload