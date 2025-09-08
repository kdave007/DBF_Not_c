
from .send_request import SendRequest
from .send_details import SendDetails
from .api_response_tracking import APIResponseTracking
from src.config.db_config import PostgresConnection
from src.db.retries_tracking import RetriesTracking
from src.db.error_tracking import ErrorTracking
from datetime import datetime, date
import os
import sys
import time
import logging
from dotenv import load_dotenv
from src.utils.get_enc import EncEnv
from src.controllers.pending_records_controller import PendingRecordsController
from src.controllers.get_pendings_req import GetPendingReq

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
        self.pending_records = PendingRecordsController(self.db_config)
        self.get_pending = GetPendingReq()
        self.sql_enabled = self.env.get('SQL_ENABLED', 'True').lower() == 'true'

        self.bypass_ca = False

        if "create" in operations:
            create_results = self._create(operations['create'])
            logging.info(f"request to upload data finished")
            #wait here till the server process the documents...
            # sys.exit()
            time.sleep(2)
            logging.info(f"START ::  GET request for documents uploaded to the server...")
            update_results = self.update_pending()

            logging.info(f"RESUME :: ")
            logging.info(f"//***// Total POST successfull op {create_results['success']}, total failed op {create_results['failed']} //***//")
            logging.info(f"//***// Total GET successfull op {update_results['success']}, total failed op {update_results['failed']} //***//")

            

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
                waiting_line_result = self.send_req.waiting_line(record, base_url, api_key)

                print(f"waiting line result : {waiting_line_result}")
                # sys.exit()
                
                # Check if the first request was successful
                if waiting_line_result['success']:
                    total_successfull_op += 1
                    print(f"Successfully processed request for folio: {record.get('folio')}")
                    #insert in the db the posted CA record
                    if sql_enabled :
                        fac_result = self.api_track._create_op(waiting_line_result['success'][0])
                        logging.info(f"insertion sql headers success: {fac_result}")
                        # Process partidas (details)
                        details_result = self.api_track._details_waiting(waiting_line_result['success'][0])
                        print(f"Details processing result: {details_result}")
                        logging.info(f"insertion sql details success: {details_result}")

                        if details_result == 0:
                            logging.info(f"insertion sql details success: {details_result}")
                            logging.info(f"When error happened, json : {waiting_line_result['success'][0].get('json_resp')}")
                        
                        # Process recibos (receipts)
                        receipts_result = self.api_track._receipts_waiting(waiting_line_result['success'][0])
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

                    if waiting_line_result.get('failed') and len(waiting_line_result['failed']) > 0:
                        # Use double quotes for outer string and ensure safe access to json_resp
                        logging.info(f"Response when error happened :: {waiting_line_result['failed'][0].get('json_resp', 'No JSON response available')}")
                    
                    if self.sql_enabled :
                        if waiting_line_result.get('failed') and len(waiting_line_result['failed']) > 0 and waiting_line_result['failed'][0].get('error_msg'):
                            self.error.insert(f"Failed process folio: {record.get('folio')}, "+f"{ waiting_line_result['failed'][0]['error_msg']}", self.class_name)
                        
                
                    if waiting_line_result['failed']:
                        for failure in waiting_line_result['failed']:
                            print(f"Failure reason: {failure.get('error_msg')}")
                    # Skip to next record if first request failed

                    #update retry
                    if self.sql_enabled :
                        self._retry_tracker(record)

                    continue
        # logging.info(f"//***// Total create successfull op {total_successfull_op}, total failed op {total_failed_op} //***//")
        return {'success' : total_successfull_op, 'failed' : total_failed_op}
            
    def update_pending(self):
        """TODO: 
            read values and just update status and ids for each partition, create an update status and id method for ca, pa, and co
        """
        total_successfull_op = 0
        total_failed_op = 0
        
        pendings = self.pending_records.get_pending_records('DV')

        print(f'pendings {pendings}')

        for record in pendings:

            #DEBUG RETURN, DELETE AFTER TESTING ---------------------------------------------------------
            # if total_successfull_op > 3 or total_failed_op > 3:
            #     return {'success' : total_successfull_op, 'failed' : total_failed_op}

            folio = record.get('num_doc')
            results = self.get_pending.send(record)

            print(f'GET REQUEST  {folio}')

            if results['success']:
                total_successfull_op += 1
                print(f"Successfully processed GET request for folio: {folio}")

                if self.sql_enabled :
                
                    fac_result = self.api_track._head_completed(results['success'][0])
                    logging.info(f"insertion sql headers success: {fac_result}")
                   
                    
                    # Process partidas (details)
                    details_result = self.api_track._detail_completed(results['success'][0])
                    logging.info(f"insertion sql details succes: {details_result}")
                    

                    if details_result == 0:
                        logging.info(f"insertion sql details success: {details_result}")
                        logging.info(f"When error happened, json : {results['success'][0].get('json_resp')}")
                    
                    
                    # Process recibos (receipts)
                    receipts_result = self.api_track._receipt_completed(results['success'][0])
                    print(f"Receipts processing result: {receipts_result}")
                    logging.info(f"insertion sql receipts success: {receipts_result}")

                    self._retry_completed(record)
            
            else :
                print(f"Failed to process GET request for folio: {folio}")
                logging.error(f"Failed to process GET request for folio: {folio}")
                total_failed_op += 1

                if results.get('failed') and len(results['failed']) > 0:
                    # Use double quotes for outer string and ensure safe access to json_resp
                    logging.info(f"Response when error happened :: {results['failed'][0].get('json_resp', 'No JSON response available')}")
                
                self._retry_tracker(record)

                continue
            

        return {'success' : total_successfull_op, 'failed' : total_failed_op}
    
                

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
    

    


