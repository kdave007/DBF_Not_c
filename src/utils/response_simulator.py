import json
import random
from datetime import datetime

class ResponseSimulator:
    """
    Class to simulate API responses for debugging and testing purposes.
    Generates dynamic responses based on the input data structure.
    """
    
    @staticmethod
    def simulate_response(dbf_record, folio, status_code=200):
        """
        Simulate an API response based on the provided DBF record.
        
        Args:
            dbf_record (dict): The DBF record containing details and receipts
            folio (str): The folio number for the record
            status_code (int): HTTP status code to simulate
            
        Returns:
            tuple: (status_code, response_json)
        """
        # Generate random IDs for the response
        ca_id = random.randint(1, 100)
        id_cta_cor_t = random.randint(1, 100)
        id_dtl_doc_cob_t = random.randint(1, 100)
        id_rbo_cob_t = random.randint(1, 100)
        
        # Create the base response structure with new format
        response = {
            "CA": {
                "id": ca_id,
                "folio": int(folio) if folio.isdigit() else folio
            },
            "MENSAJE": "",
            "STATUS": "OK",
            "PA": [],
            "CO": {
                "ID_CTA_COR_T": id_cta_cor_t,
                "ID_DTL_DOC_COB_T": id_dtl_doc_cob_t,
                "ID_RBO_COB_T": id_rbo_cob_t,
                "ID_DTL_COB_APL_T": []
            }
        }
        
        # Generate PA (partidas) entries based on details in the record
        details = dbf_record.get('detalles', [])
        for i, detail in enumerate(details, 1):
            pa_entry = {
                "id": random.randint(1, 1000),
                "_indice": i
            }
            response["PA"].append(pa_entry)
        
        # Generate ID_DTL_COB_APL_T entries based on receipts in the record
        receipts = dbf_record.get('recibos', [])
        for i, receipt in enumerate(receipts, 1):
            dtl_cob_apl_entry = {
                "ID_DTL_COB_APL_T": random.randint(1, 1000),
                "_indice": i
            }
            response["CO"]["ID_DTL_COB_APL_T"].append(dtl_cob_apl_entry)
        
        return status_code, response
    
    @staticmethod
    def create_mock_response(status_code, json_data):
        """
        Create a mock response object that mimics requests.Response
        
        Args:
            status_code (int): HTTP status code
            json_data (dict): JSON data to return from .json() method
            
        Returns:
            MockResponse: A mock response object
        """
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.json_data = json_data
                self.status_code = status_code
                self.text = json.dumps(json_data)
                self.headers = {
                    'Content-Type': 'application/json',
                    'Date': datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')
                }
            
            def json(self):
                return self.json_data
        
        return MockResponse(json_data, status_code)
