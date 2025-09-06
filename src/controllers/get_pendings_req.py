import requests
from src.utils.get_enc import EncEnv
from src.utils.response_simulator import ResponseSimulator
import logging
import os
import sys
import json

class GetPendingReq:

    def __init__(self):
        self.env = EncEnv()
        self.get_endpoint= self.env.get("API_GET_URL")# THIS MUST BE A NEW URL
        self.api = self.env.get("API_KEY")
        self.DEBUG_MODE = self.env.get('DEBUG_MODE', 'True').lower() == 'true'

    def send(self, record):
        
        results = {
            'success': [],  # Will store folio -> result for successful operations
            'failed': []   # Will store folio -> result for failed operations
        }

        headers = {
            "Content-Type": "application/json",
         
             "accept": "*/*",
  
            "accept-encoding": "gzip, deflate, br",
        }

    
        print(f'GET record  {record}')
        # Extract parameters from the record
        waiting_id = record.get('id')
        folio = record.get('num_doc')
        serie = record.get('serie')
        fecha = record.get('fecha')
        
        # Construct URL with query parameters

        url = f"{self.get_endpoint}?api_key={self.api}&params[NUM_DOC]={folio}&params[SER]={serie}&params[FCH]={fecha}"
        # url = f"https://c8.velneo.com:17262/api/vLatamERP_db_dat/v2/_process/PRO_VTA_FAC_JSON?api_key={self.api}&params[NUM_DOC]={folio}&params[SER]={serie}&params[FCH]={fecha}-05-16"
        # Log the request
        print(f"Making GET request to: {url}")
        logging.info(f"GET REQUEST for FOLIO {folio} ")
        logging.info(f"waiting ID : {waiting_id} - serie {serie} - fecha {fecha}")

        if self.DEBUG_MODE:
            logging.info("DEBUG MODE ON :: Response simulated ")

            status_code, response_json = ResponseSimulator.simulate_response(record, folio)
            response = ResponseSimulator.create_mock_response(status_code, response_json)  

        else:  
            # Make the GET request
            try:
                response = requests.get(url, headers=headers, timeout=60)

            except requests.exceptions.Timeout:
                error_msg = f"get_pendings_req :: Request timed out after 60 seconds for folio {folio}"
                logging.error(error_msg)
                print(error_msg)
                # Return a failed result instead of raising an exception
                return {
                    'success': [],
                    'failed': [{
                        'folio': folio,
                        'error_msg': error_msg,
                        'json_resp': None
                    }]
                }

            except requests.exceptions.ConnectionError as e:
                error_msg = f"get_pendings_req :: Connection error for folio {folio}: {str(e)}"
                logging.error(error_msg)
                print(error_msg)
                # Return a failed result instead of raising an exception
                return {
                    'success': [],
                    'failed': [{
                        'folio': folio,
                        'error_msg': error_msg,
                        'json_resp': None
                    }]
                }
                
            except Exception as e:
                error_msg = f"get_pendings_req :: Unexpected error for folio {folio}: {str(e)}"
                logging.error(error_msg)
                print(error_msg)
                # Return a failed result instead of raising an exception
                return {
                    'success': [],
                    'failed': [{
                        'folio': folio,
                        'error_msg': error_msg,
                        'json_resp': None
                    }]
                }
        
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
                
                estados = {
                    "CA":"completado",
                    "PA":"completado",
                    "CO":"completado"
                }
                

                # Check if the response has the expected structure
                if 'ST' not in response_json or response_json['ST'] != 'OK':
                    logging.info(f"Response Status Code for folio {folio}: {response_json}")
                    print(f"Invalid response status for folio {folio}. Full response: {response_json}")
                    raise ValueError(" get_pendings_req :: Invalid response status in response")
                
                # Validate that CA exists and is not empty
                if 'CA' not in response_json:
                    logging.error(f"Missing CA section in response for folio {folio}")
                    print(f"Missing CA section in response for folio {folio}")
                    raise ValueError(" get_pendings_req :: Invalid response format: CA section is missing")
                elif not response_json['CA'] or (isinstance(response_json['CA'], dict) and len(response_json['CA']) == 0):
                    logging.error(f"Empty CA section in response for folio {folio}")
                    print(f"Empty CA section in response for folio {folio}")
                    raise ValueError(" get_pendings_req :: Invalid response format: Empty CA section in response")

                # Validate that PA exists and is not empty
                if 'PA' not in response_json:
                    logging.error(f"Missing PA section in response for folio {folio}")
                    print(f"Missing PA section in response for folio {folio}")
                    raise ValueError(" get_pendings_req :: Invalid response format: PA section is missing")
                elif not response_json['PA'] or (isinstance(response_json['PA'], list) and len(response_json['PA']) == 0):
                    logging.error(f"Empty PA section in response for folio {folio}")
                    print(f"Empty PA section in response for folio {folio}")
                    estados['CA'] = "incompleto"
                    estados['PA'] = "error"

                # Validate that CO exists and is not empty
                # if 'CO' not in response_json:
                #     logging.error(f"Missing CO section in response for folio {folio}")
                #     print(f"Missing CO section in response for folio {folio}")
                #     raise ValueError(" get_pendings_req :: Invalid response format: CO section is missing")
                # elif not response_json['CO'] or (isinstance(response_json['CO'], dict) and len(response_json['CO']) == 0):
                #     logging.error(f"Empty CO section in response for folio {folio}")
                #     print(f"Empty CO section in response for folio {folio}")
                #     estados['CA'] = "incompleto"
                #     estados['CO'] = "error"
                
                
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
                        'accion':"registrado",
                        'estado':estados['CA'],
                        'status': response.status_code,
                        'partidas': [],
                        'recibos': [],
                        'json_resp': formatted_json
                    }
                    
                    # Process PA (Partidas) data
                    if 'PA' in response_json and isinstance(response_json['PA'], list):
                        for partida in response_json['PA']:
                            indice = partida.get('_indice')
                            
                            partida_data = {
                                'id': partida.get('id'),
                                'indice': indice,
                                'folio': folio,
                                'estado': estados['PA']
                            }
                            
                            success_entry['partidas'].append(partida_data)
                    
                    # Process CO (RECIBOS COBRADOS) data with new structure
                    # if 'CO' in response_json and isinstance(response_json['CO'], dict):
                    #     # Convert CO section to a JSON string
                    #     for receipt in response_json['CO']:

                    #         resp_string = json.dumps(response_json['CO'])
                    #         recibos_data = {
                    #             'id': id_value,
                    #             'folio': folio,
                    #             'respuesta': resp_string,
                    #             'estado': estados['CO']
                    #         }
                        
                    #         success_entry['recibos'].append([])
                    
                    # Add to success results
                
                    results['success'].append(success_entry)
                    
                    print(f"Successfully processed response for folio {folio_str}")
                    # logging.info(f"Successfully processed response for folio {folio_str}")
            except Exception as e:
                print(f"get_pendings :: Error processing response for folio   {folio}: {(e)}")
                logging.info(f"get_pendings :: Error processing response for folio   {folio}: {(e)}")
                # Add to failed results
                results['failed'].append({
                    'folio': folio,
                    'fecha_emision': record.get('fecha'),
                    'total_partidas': len(record.get('detalles', [])),
                    'hash': record.get('dbf_hash', ''),
                    'status': response.status_code,
                    'error_msg': f"Error processing response: {str(e)}",
                    'json_resp':formatted_json
                })
        else:
            # Failed request
            error_message = f"Request failed with status {response.status_code}: {response.text}"
            print(f"get_pendings :: Error for folio {folio}: {error_message}")
            logging.info(f" get_pendings :: Request failed with status {response.status_code}: {response.text}")
            
            results['failed'].append({
                'folio': folio,
                'fecha_emision': record.get('fecha'),
                'total_partidas': len(record.get('detalles', [])),
                'hash': record.get('dbf_hash', ''),
                'status': response.status_code,
                'error_msg': error_message
            })
    
    
        return results