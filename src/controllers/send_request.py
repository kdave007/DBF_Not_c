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

# Load environment variables
load_dotenv()

# Set debug flag from .env - set to True to use simulated responses instead of real API calls
DEBUG_MODE = os.getenv('DEBUG_MODE', 'True').lower() == 'true'

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

    # def send(self, responses_dict):
    #     """Process API operations in batches of 100 and track results"""
    #     # responses_dict contains API operation results (update, delete, create)
    #     if not responses_dict:
    #         return False
        
    #     # API configuration
    #     self.base_url = "http://localhost:3000/api/data"  # Replace with your actual API URL
    #     self.base_url = "https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/vta_fac_g"
    #     self.api_key = "123456"
    #     self.table_name = "vta_fac_g"
    #     self.headers = {
    #         "Content-Type": "application/json",
    #         "Accept": "application/json",
    #         "x-process-json": "true"
    #     }
    #     self.batch_size = 100
    #     self.json_encoder = CustomJSONEncoder
        
    #     # Separate operations by type
    #     creates = responses_dict.get('create', [])
    #     updates = responses_dict.get('update', [])
    #     deletes = responses_dict.get('delete', [])
    #     print("delete:")
    #     print(deletes)
        

    #     # Initialize results dictionary
    #     results = {
    #         'create': {},
    #         'update': {},
    #         'delete': {},
    #         'next_check':responses_dict.get('next_check', [])
    #     }
        
    #     # Process in batches
    #     if creates:
    #         print(f' CREATE PROCESS >>')
    #         result_create = self.create(creates)
    #         results['create'] = result_create
    #         # print("Creates:")
    #         # print(json.dumps(result_create, indent=4, cls=self.json_encoder))

    #     if updates:  
    #         print(f' UPDATE PROCESS >>')  
    #         result_update = self.update(updates)
    #         results['update'] = result_update
    #         # print("Updates:")
    #         # print(json.dumps(result_update, indent=4, cls=self.json_encoder))

    #     if deletes:
    #         print(f' DELETE PROCESS >>')
    #         result_delete = self.delete(deletes)
    #         results['delete'] = result_delete
    #         # print("Deletes:")
    #         # print(json.dumps(result_delete, indent=4, cls=self.json_encoder))
        
    #     return results

    def create(self, record, base_url, api_key):
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

                # Prepare payload for a single record
                single_payload = {
                    "emp": str(dbf_record.get('emp')),
                    "emp_div": str(dbf_record.get('emp_div')),
                    "num_doc": folio,
                    "clt": dbf_record.get('clt'),
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
                    "detalles": self._format_details(dbf_record),
                    "recibos": self._format_receipts(dbf_record),
                    "usr":1,
                    "aut_usr":1,
                    "usr":1,
                    "por_dto":0,
                    "vta_fac_g": dbf_record.get('vta_fac_g')
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
                status_code, response_json = ResponseSimulator.simulate_response(dbf_record, folio)
                response = ResponseSimulator.create_mock_response(status_code, response_json)
            else:
                # Make actual API request
                response = requests.post(
                    f"{base_url}?api_key={api_key}", 
                    headers=headers,
                    data=post_data
                )
            
            print(f"Response Status Code for folio {folio}: {response.status_code}")
            print(f"Response Headers for folio {folio}: {response.headers}")

            logging.info(f"Response Status Code for folio {folio}: {response.status_code}")
            
            # Process the response

         

            if response.status_code in [200, 201, 202, 204]:
                try:
                    # Parse response JSON
                    response_json = response.json()
                    formatted_json = json.dumps(response_json, indent=4, sort_keys=False)
                    print(f"Response JSON for folio {folio}:\n{formatted_json}")
                    
                   
                    # Check if the response has the expected structure
                    if 'STATUS' not in response_json or response_json['STATUS'] != 'OK':

                        logging.info(f"Response Status Code for folio {folio}: {response_json}")
                        print(f"Invalid response status for folio {folio}. Full response: {response_json}")
                        raise ValueError("Invalid response status in response")
                    
                    # Process CA (Cabecera) data
                    if 'CA' in response_json and response_json['CA']:
                        ca_data = response_json['CA']
                        id_value = ca_data.get('id')
                        folio_str = str(ca_data.get('folio'))
                        logging.info(f"Response fac id {id_value}")
                        
                        # Create success entry
                        success_entry = {
                            'folio': folio_str,
                            'id': id_value,
                            'fecha_emision': dbf_record.get('fecha'),
                            'total_partidas': len(dbf_record.get('detalles', [])),
                            'hash': record.get('dbf_hash', ''),
                            # 'details': dbf_record.get('detalles', []),
                            # 'receipts': dbf_record.get('recibos', []),
                            'status': response.status_code,
                            'partidas': [],
                            'recibos': []
                        }
                        
                        # Process PA (Partidas) data
                        if 'PA' in response_json and isinstance(response_json['PA'], list):
                            for partida in response_json['PA']:
                                indice = partida.get('_indice')
                                # Find matching detail in dbf_record.get('detalles', []) based on _indice
                                matching_detail = None
                                if indice is not None and indice > 0 and len(dbf_record.get('detalles', [])) >= indice:
                                    # _indice is 1-based, but list indices are 0-based
                                    matching_detail = dbf_record.get('detalles', [])[indice - 1]
                                
                                partida_data = {
                                    'id': partida.get('id'),
                                    'indice': indice,
                                    'folio': folio,
                                }
                                
                                # Add additional fields from matching detail if found
                                if matching_detail:
                                    # Add art from the matching detail
                                    partida_data['art'] = matching_detail.get('art', '')
                                    partida_data['detail_hash'] = matching_detail.get('detail_hash', '')
                                    # Check for REF in both uppercase and lowercase keys
                                    if 'REF' in matching_detail:
                                        partida_data['ref'] = matching_detail['REF']
                                    elif 'ref' in matching_detail:
                                        partida_data['ref'] = matching_detail['ref']
                                    else:
                                        partida_data['ref'] = ''
                                
                                success_entry['partidas'].append(partida_data)
                        
                        # Process CO (RECIBOS COBRADOS) data with new structure
                        if 'CO' in response_json and isinstance(response_json['CO'], dict) and dbf_record.get('recibos', []):
                            co_data = response_json['CO']
                    
                            
                            # Extract common IDs from CO object
                            id_cta_cor_t = co_data.get('ID_CTA_COR_T')
                            id_dtl_doc_cob_t = co_data.get('ID_DTL_DOC_COB_T')
                            id_rbo_cob_t = co_data.get('ID_RBO_COB_T')
                            
                            # Process ID_DTL_COB_APL_T array which contains receipt mappings
                            dtl_cob_apl_entries = co_data.get('ID_DTL_COB_APL_T', [])
                            
                            for receipt_entry in dtl_cob_apl_entries:
                                # Get the _indice from the receipt entry
                                indice = receipt_entry.get('_indice')
                                
                                # Initialize receipt data with basic fields and all IDs
                                receipt_data = {
                                    'id_dtl_cob_apl_t': receipt_entry.get('ID_DTL_COB_APL_T'),
                                    'id_cta_cor_t': id_cta_cor_t,
                                    'id_dtl_doc_cob_t': id_dtl_doc_cob_t,
                                    'id_rbo_cob_t': id_rbo_cob_t,
                                    'id_fac': id_value,
                                    'folio': folio_str,  # From CA
                                }
                                
                                # Find matching receipt in dbf_record.get('recibos', []) based on _indice
                                matching_receipt = None
                                if indice is not None and 1 <= indice <= len(dbf_record.get('recibos', [])):
                                    matching_receipt = dbf_record.get('recibos', [])[indice - 1]
                                
                                # Add additional fields from matching receipt if found
                                if matching_receipt:
                                    # Add num_ref from the matching receipt
                                    receipt_data['num_ref'] = matching_receipt.get('ref_recibo', '')
                                    # Add fecha from the matching receipt (fch)
                                    fecha = matching_receipt.get('fch', dbf_record.get('fecha'))
                                    # Ensure fecha is a date object without time component
                                    if isinstance(fecha, datetime):
                                        receipt_data['fecha_emision'] = fecha.date()
                                    else:
                                        receipt_data['fecha_emision'] = fecha
                                else:
                                    receipt_data['num_ref'] = ''
                                    fecha = dbf_record.get('fecha')
                                    # Ensure fecha is a date object without time component
                                    if isinstance(fecha, datetime):
                                        receipt_data['fecha_emision'] = fecha.date()
                                    else:
                                        receipt_data['fecha_emision'] = fecha
                                
                                success_entry['recibos'].append(receipt_data)
                        
                        # Add to success results
                        results['success'].append(success_entry)
                        
                        print(f"Successfully processed response for folio {folio_str}")
                        logging.info(f"Successfully processed response for folio {folio_str}")
                except Exception as e:
                    print(f"Error processing response for folio   {folio}: {(e)}")
                    logging.info(f"Error processing response for folio   {folio}: {(e)}")
                    # Add to failed results
                    results['failed'].append({
                        'folio': folio,
                        'fecha_emision': dbf_record.get('fecha'),
                        'total_partidas': len(dbf_record.get('detalles', [])),
                        'hash': record.get('dbf_hash', ''),
                        'status': response.status_code,
                        'error_msg': f"Error processing response: {str(e)}"
                    })
            else:
                # Failed request
                error_message = f"Request failed with status {response.status_code}: {response.text}"
                print(f"Error for folio {folio}: {error_message}")
                logging.info(f"Request failed with status {response.status_code}: {response.text}")
                
                results['failed'].append({
                    'folio': folio,
                    'fecha_emision': dbf_record.get('fecha'),
                    'total_partidas': len(dbf_record.get('detalles', [])),
                    'hash': record.get('dbf_hash', ''),
                    'status': response.status_code,
                    'error_msg': error_message
                })
                
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

    # def update(self, update):
    #     results = {
    #         'success': [],  # Will store folio -> result for successful operations
    #         'failed': []   # Will store folio -> result for failed operations
    #     }
        
    #     print(f"Processing {len(update)} UPDATE operations in batches of {self.batch_size}")
    #     for i in range(0, len(update), self.batch_size):
    #         batch = update[i:i+self.batch_size]
    #         print(f"Processing UPDATE batch {i//self.batch_size + 1} with {len(batch)} operations")
            
    #         try:
    #             # Prepare batch payload
    #             batch_payload = []
    #             folio_to_item = {}

    #             current_folio = None

    #             # Process each item in the batch individually
    #             for item in batch:
    #                 folio = item.get('folio')
    #                 folio_to_item[folio] = item
    #                 dbf_record = item.get('dbf_record', {})
                
    #                 # Prepare payload for a single record
    #                 single_payload = {
    #                     "id":item.get("id"),
    #                     "emp":str(dbf_record.get('emp')),
    #                     "emp_div": str(dbf_record.get('emp_div')),
    #                     "num_fac": f'VTA/-"{folio}',
    #                     "num_doc":folio,
    #                     "clt": dbf_record.get('clt'),
    #                     "fpg": dbf_record.get('fpg'),
    #                     "cmr": dbf_record.get('cmr'),
    #                     "fch": self._format_date_to_iso(dbf_record.get("fecha")),
    #                     "tot_fac": dbf_record.get("total_bruto"),
    #                     "ser":dbf_record.get('ser'),
    #                     "hor":self._format_hour_to_12h(dbf_record.get('hor')),
    #                     "pai":dbf_record.get('pai'),
    #                     "ent_rel_tip":1,
    #                     "mon_c":1,
    #                     "cot":1,
    #                     "fch_vto":self._format_date_to_iso(dbf_record.get("fecha")),
    #                     "pre_con_iva_inc":1,
    #                     "trm":1,
    #                     "dum":1,
    #                     "off":1
    #                 }
                    
    #                 # Send the single record
    #                 print(f"Sending record for folio {folio}")
    #                 post_data = json.dumps(single_payload, cls=CustomJSONEncoder)
    #                 print(f"POST Request URL: {self.base_url}/{item.get("id")}?api_key={self.api_key}")
    #                 print(f"POST Request Data: {post_data}")
                    
    #                 response = requests.post(
    #                     f"{self.base_url}/{item.get("id")}?api_key={self.api_key}", 
    #                     headers=self.headers, 
    #                     data=post_data
    #                 )
                    
    #                 print(f"Response Status Code for folio {folio}: {response.status_code}")
    #                 print(f"Response Headers for folio {folio}: {response.headers}")
                    
    #                 # Process this individual response immediately
    #                 if response.status_code in [200, 201, 202, 204]:
    #                     try:
    #                         # Parse response JSON
    #                         response_json = response.json()
    #                         print(f"Response JSON for folio {folio}: {response_json}")
                            
    #                         if 'vta_fac_g' not in response_json:
    #                             print(f"Key 'vta_fac_g' not found in response for folio {folio}. Full response: {response_json}")
    #                             continue
                                
    #                         if not response_json['vta_fac_g']:
    #                             print(f"'vta_fac_g' is empty for folio {folio}. Full response: {response_json}")
    #                             continue
                                
    #                         # Process each item in the response
    #                         for resp_item in response_json['vta_fac_g']:
    #                             id_value = resp_item.get('id')
    #                             folio_str = str(resp_item.get('num_doc'))
                                
    #                             # Find the original item
    #                             original_item = folio_to_item.get(folio_str)
    #                             if not original_item:
    #                                 print(f"Warning: Could not find original item for folio {folio_str}")
    #                                 continue
                                    
    #                             dbf_record = original_item.get('dbf_record', {})
                                
    #                             # Add to success results
    #                             results['success'].append({
    #                                 'folio': folio_str,
    #                                 'id': id_value,
    #                                 'fecha_emision': dbf_record.get('fecha'),
    #                                 'total_partidas': len(dbf_record.get('detalles', [])),
    #                                 'hash': original_item.get('dbf_hash', ''),
    #                                 'details': dbf_record.get('detalles', []),
    #                                 'status': response.status_code
    #                             })
                                
    #                             print(f"Successfully processed response for folio {folio_str}")
    #                     except Exception as e:
    #                         print(f"Error processing response for folio {folio}: {str(e)}")
    #                         # Add to failed results
    #                         results['failed'].append({
    #                             'folio': folio,
    #                             'fecha_emision': dbf_record.get('fecha'),
    #                             'total_partidas': len(dbf_record.get('detalles', [])),
    #                             'hash': item.get('dbf_hash', ''),
    #                             'status': response.status_code,
    #                             'error_msg': f"Error processing response: {str(e)}"
    #                         })
    #                 else:
    #                     # Failed request
    #                     error_message = f"Request failed with status {response.status_code}: {response.text}"
    #                     print(f"Error for folio {folio}: {error_message}")
                        
    #                     results['failed'].append({
    #                         'folio': folio,
    #                         'fecha_emision': dbf_record.get('fecha'),
    #                         'total_partidas': len(dbf_record.get('detalles', [])),
    #                         'hash': item.get('dbf_hash', ''),
    #                         'status': response.status_code,
    #                         'error_msg': error_message
    #                     })
    #             # All response processing is now done individually for each record
                        
    #         except Exception as e:
    #             error_message = f"Exception during batch create: {str(e)}"
    #             print(error_message)
              
    #             # Mark all records in the batch as failed
    #             for item in batch_payload:
    #                     original_item = folio_to_item.get(folio)
    #                     dbf_record = original_item.get('dbf_record', {})
    #                     error_message = f"Batch create failed with status {response.status_code}: {response.text}"
                        
    #                     results['failed'].append({
    #                         'folio': item.get('folio'), 
    #                         'fecha_emision':  dbf_record.get('fecha'),
    #                         'hash': original_item.get('dbf_hash', ''),
    #                         'status': None,
    #                         'error_msg':error_message
    #                         })
                  
    #     return results
    

    # def delete(self, deletes):
    #     results = {
    #         'success': [],  # Will store folio -> result for successful operations
    #         'failed': []   # Will store folio -> result for failed operations
    #     }
        
    #     print(f"Processing {len(deletes)} DELETE operations in batches of {self.batch_size}")
    #     for i in range(0, len(deletes), self.batch_size):
    #         batch = deletes[i:i+self.batch_size]
    #         print(f"Processing DELETE batch {i//self.batch_size + 1} with {len(batch)} operations")
            
    #         try:
    #             # Prepare batch payload
    #             batch_payload = []
    #             folio_to_item = {}

    #             for item in batch:
    #                 print(item)
    #                 folio = item.get('folio')

    #                 folio_to_item[folio] = item
    #                 dbf_record = item.get('dbf_record', {})
    #                 batch_payload.append({
    #                     "folio": folio,
    #                     "cabecera": dbf_record.get("Cabecera"),
    #                     "cliente": dbf_record.get("cliente"),
    #                     "empleado": dbf_record.get("empleado"),
    #                     "fecha": dbf_record.get("fecha"),
    #                     "total_bruto": dbf_record.get("total_bruto")
    #                 })
                
    #             # Extract folios for the URL
    #             folios = [item.get('folio') for item in batch]
    #             ids = [str(item.get('id')) for item in batch]
                
    #             # If only one element, use the ID directly; otherwise join with comma
    #             if len(ids) == 1:
    #                 all_ids = ids[0]
    #             else:
    #                 all_ids = "%2C".join(ids)

    #             # Send the single record
    #             print(f"Sending record for folio {folio}")
    #             #post_data = json.dumps(single_payload, cls=CustomJSONEncoder)
    #             print(f"POST Request URL: {self.base_url}/{all_ids}?api_key={self.api_key}")
               
                    
                
    #             # Make batch DELETE request with folios in the URL
    #             response = requests.delete(
    #                 f"{self.base_url}/{all_ids}?api_key={self.api_key}", 
    #                 headers=self.headers
    #             )

    #             print(f"Response Status Code for folio {folio}: {response.status_code}")
    #             print(f"Response Headers for folio {folio}: {response.headers}")
                
    #             if response.status_code in [200, 201, 202, 204]:
    #                 # For DELETE operations, the response might be different from create/update
    #                 # It could be a list of deleted IDs or a success message
    #                 try:
    #                     batch_response = response.json()
    #                     print(batch_response)
                        
    #                     # Check for the specific success response format: {"return": "Eliminado(s) con éxito"}
    #                     if isinstance(batch_response, dict) and batch_response.get('return') == "Eliminado(s) con éxito":
    #                         # Success response for batch deletion - all items were successfully deleted
    #                         for folio in folios:
    #                             if folio in folio_to_item:
    #                                 original_item = folio_to_item.get(folio)
    #                                 dbf_record = original_item.get('dbf_record', {})
                                    
    #                                 results['success'].append({
    #                                     'folio': folio,
    #                                     'id': original_item.get('id'),
    #                                     'status': response.status_code
    #                                 })
    #                     # If the response contains a list of deleted items
    #                     elif isinstance(batch_response, list):
    #                         for deleted_item in batch_response:
    #                             folio = deleted_item.get('folio')
    #                             if folio and folio in folio_to_item:
    #                                 original_item = folio_to_item.get(folio)
    #                                 dbf_record = original_item.get('dbf_record', {})
                                    
    #                                 results['success'].append({
    #                                     'folio': folio,
    #                                     'id': deleted_item.get('id'),
    #                                     'status': response.status_code
    #                                 })
    #                     # If the response is any other format
    #                     else:
    #                         # Consider all items in the batch as successfully deleted
    #                         for folio in folios:
    #                             if folio in folio_to_item:
    #                                 original_item = folio_to_item.get(folio)
                                    
    #                                 results['success'].append({
    #                                     'folio': folio,
    #                                     'id': original_item.get('id'),
    #                                     'status': response.status_code
    #                                 })
    #                 except ValueError:
    #                     # If the response is not JSON, consider all items successful
    #                     for folio in folios:
    #                         if folio in folio_to_item:
    #                             original_item = folio_to_item.get(folio)
    #                             dbf_record = original_item.get('dbf_record', {})
                                
    #                             results['success'].append({
    #                                 'folio': folio,
    #                                 'id': original_item.get('id'),
    #                                 'status': response.status_code
    #                             })
        
    #             else:
    #                 error_message = f"Batch delete failed with status {response.status_code}: {response.text}"
    #                 # Mark all items in the batch as failed
    #                 for folio in folios:
    #                     if folio in folio_to_item:
    #                         original_item = folio_to_item.get(folio)
    #                         dbf_record = original_item.get('dbf_record', {})
                            
    #                         results['failed'].append({
    #                             'folio': folio,
    #                             'fecha_emision': dbf_record.get('fecha'),
    #                             'total_partidas': len(dbf_record.get('detalles', [])),
    #                             'hash': original_item.get('dbf_hash', ''),
    #                             'status': response.status_code,
    #                         'error_msg':error_message
    #                         })
                        
    #         except Exception as e:
    #             error_message = f"Exception during batch create: {str(e)}"
    #             print(f' error message {error_message}')
    #             # Mark all records in the batch as failed
    #             for item in batch_payload:
    #                     error_message = f"Batch create failed with status {response.status_code}: {response.text}"
    #                     print(f'item aa {item}')
    #                     results['failed'].append({
    #                         'folio': item.get('folio'), 
    #                         'status': None,
    #                         'error_msg':error_message
    #                         })
                  

    #     return results

        
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