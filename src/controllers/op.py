
from .send_request import SendRequest
from .send_details import SendDetails
from .api_response_tracking import APIResponseTracking
from src.config.db_config import PostgresConnection
from src.db.retries_tracking import RetriesTracking
from src.db.error_tracking import ErrorTracking
from datetime import datetime, date
import os
import sys
import logging
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv

# Load environment variables
load_dotenv()

class OP:
    def execute(self, operations):
        self.class_name = "Op"
        self.send_req = SendRequest()
        self.send_det = SendDetails()
        self.api_track = APIResponseTracking()
        self.db_config = PostgresConnection.get_db_config()
        self.retries_track = RetriesTracking(self.db_config)
        self.error = ErrorTracking(self.db_config)
        self.env = EncEnv()

        self.bypass_ca = False

        if "create" in operations:
            ops = self._create(operations['create'])
            logging.info(f"request to upload data finished")
            

        if "update" in operations:
            pass
        #    self._update(operations['update'])

        if "delete" in operations:
            pass
            # self._delete(operations['delete'])     


    def _create(self, records):
        # Read API configuration from .env file
        base_url = self.env.get('API_BASE_URL')
        api_key = self.env.get('API_KEY')

        total_successfull_op = 0
        total_failed_op = 0
        
        # Check if SQL operations are enabled
        sql_enabled = self.env.get('SQL_ENABLED', 'True').lower() == 'true'
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')
            
            
            if self.bypass_ca:
                print(f"Bypassing first API call for folio: {record.get('folio')}")
                 
            else:
                # Make the first API call
                ca_req_result = self.send_req.create(record, base_url, api_key)


                print(f' req result -----> {ca_req_result}')
                # sys.exit()
                
                # Check if the first request was successful
                if ca_req_result['success']:
                    total_successfull_op += 1
                    print(f"Successfully processed request for folio: {record.get('folio')}")
                    #insert in the db the posted CA record
                    if sql_enabled :
                        fac_result = self.api_track._create_op(ca_req_result['success'][0])
                        logging.info(f"insertion sql headers success: {fac_result}")
                        # Process partidas (details)
                        details_result = self.api_track._details_completed(ca_req_result['success'][0])
                        print(f"Details processing result: {details_result}")
                        logging.info(f"insertion sql details success: {details_result}")
                        
                        # Process recibos (receipts)
                        receipts_result = self.api_track._receipts_completed(ca_req_result['success'][0])
                        print(f"Receipts processing result: {receipts_result}")
                        logging.info(f"insertion sql receipts success: {receipts_result}")


                    #here insert in DB the PARTIDAS and RECIBOS 
                     #update if it is a record retry    
                    if sql_enabled :
                        self._retry_completed(record)
                else:
                    print(f"Failed to process first request for folio: {record.get('folio')}")
                    logging.error(f"Failed to process request for folio: {record.get('folio')}")
                    total_failed_op += 1
                    
                    if sql_enabled :
                        self.error.insert(f"Failed process folio: {record.get('folio')}, "+f"{ ca_req_result['failed'][0]['error_msg']}", self.class_name)

                    if ca_req_result['failed']:
                        for failure in ca_req_result['failed']:
                            print(f"Failure reason: {failure.get('error_msg')}")
                    # Skip to next record if first request failed

                    #update retry
                    if sql_enabled :
                        self._retry_tracker(record)

                    continue
        logging.info(f"//***// Total create successfull op {total_successfull_op}, total failed op {total_failed_op} //***//")
        return {total_successfull_op, total_failed_op}
            
            # if first_request_success:

            #     if self.bypass_ca:
            #         parent_ref = {
            #             'parent_id':  111,
            #             'fecha': record['dbf_record'].get('fecha')
            #         }
            #     else:    
            #         parent_ref = {
            #             'parent_id':  ca_req_result['success'][0].get('id'),
            #             'fecha': ca_req_result['success'][0].get('fecha_emision')
            #         }
                
            #     print(f"Ready to process details request for folio: {record.get('folio')}")
                
            #     det_req_results = self.send_det.req_post(record['dbf_record'].get('detalles'), parent_ref)

            #     if det_req_results['failed']:
            #         #one request failed, so skip to next CA
            #         continue
                
            #     # Update the record status to indicate details were processed successfully
            #     self.api_track._pa_completed(parent_ref['parent_id'])
            #     self.api_track.update_create_details(det_req_results['records'])
                
            #     # Call after request handler
            #     emp =  record['dbf_record']['detalles'][0].get('emp')
            #     emp_div =  record['dbf_record']['detalles'][0].get('emp_div')
            #     self._after_request(parent_ref['parent_id'], emp, emp_div)
                

    def _update(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')

    def _delete(self, records):
        for record in records:
            print(f'RECORD FOUND {record}')
            print(f'------')


    def _after_request(self, id, emp, emp_div):
        self.send_det.send_update_fac_off(id, emp, emp_div)

    def _retry_tracker(self, record):
        """Track retry attempts for a record
        
        Args:
            record: The record to track
        """
        try:
            folio = record.get('folio')
            # Use fecha field for retry tracking date
            fecha_registro = None
            fecha = record['dbf_record'].get('fecha')
            
        
            try:
                # Try to parse the date string
                if isinstance(fecha, str):
                    # Extract only the date part (ignore time)
                    fecha_parts = fecha.split(' ')[0]  # Get only the date portion
                    # Format is day/month/year in the DBF records
                    fecha_registro = datetime.strptime(fecha_parts, '%d/%m/%Y').date()
                elif isinstance(fecha, date):
                    # If it's already a date object, use it directly
                    fecha_registro = fecha
            except (ValueError, AttributeError):
                # Fallback to current date if parsing fails
                fecha_registro = date.today()
                print(f"Warning: Could not parse date '{fecha}', using current date instead")
            
            if folio:
                print(f"Tracking retry attempt for folio: {folio}")
                self.retries_track.insert_or_update_fac(folio, completado=False, fecha_registro=fecha_registro)
            else:
                print("Warning: No folio found in record for retry tracking")
        except Exception as e:
            print(f"Error tracking retry for record: {e}")

    def _retry_completed(self, record):
        """Mark a record as completed in the retry tracking system
        
        Args:
            record: The record to mark as completed
        """
        try:
            folio = record.get('folio')
            if folio:
                print(f"Marking folio {folio} as completed in retry tracking")
                self.retries_track.completed(folio)
            else:
                print("Warning: No folio found in record for retry completion")
        except Exception as e:
            print(f"Error marking retry as completed for record: {e}")
    

    


