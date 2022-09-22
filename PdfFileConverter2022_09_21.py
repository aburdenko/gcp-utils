from gcputils.FileConverter import FileConverter
from gcputils.hcls_nlp import analyze_entities
from gcputils.cloud_storage import upload_str_to_bucket
from google.cloud.documentai_v1.types.document_processor_service import BatchProcessMetadata
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
#from google.cloud import documentai_v1 as documentai


class PdfFileConverter(FileConverter):
    def __init__(self
    , project_id: str
    , input_gcs_uri: str
    , first_gcs_uri : str 
    , raw_text_file_path : str
    , hcls_nl_json_uri : str    
    , updated_timestamp_str : str):
        self._location = None
        self._proc_id = None

        super().__init__( project_id
        , input_gcs_uri
        , first_gcs_uri 
        , raw_text_file_path
        , hcls_nl_json_uri 
        , updated_timestamp_str)

    @property
    def proc_id(self):
        return self._proc_id
    
    @proc_id.setter
    def proc_id(self, value):
        self._proc_id = value

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, value):
        self._location = value       

   

    def _batch_process_documents( self,
        project_id: str,
        location: str,
        processor_id: str,
        gcs_input_uri: str,
        input_mime_type: str = "application/pdf",        
        gcs_output_uri_prefix: str = 'docai_processed',
        timeout: int = 20000,
    )->BatchProcessMetadata:

        gcs_output_bucket: str = f"gs://{self._bucket_name}"
        

        # You must set the api_endpoint if you use a location other than 'us', e.g.:        
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        print("endpoint:"+opts.api_endpoint)
        client = documentai.DocumentProcessorServiceClient(client_options=opts)

        gcs_document = documentai.GcsDocument(
            gcs_uri=gcs_input_uri, mime_type=input_mime_type
        )

        # Load GCS Input URI into a List of document files
        gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
        input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

        # NOTE: Alternatively, specify a GCS URI Prefix to process an entire directory
        #
        # gcs_input_uri = "gs://bucket/directory/"
        gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_uri)
        input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
        #

        # Cloud Storage URI for the Output Directory
        #gcs_output_uri_prefix = self._input_gcs_uri
        destination_uri = f"{gcs_output_bucket}/docai_processed/"
        print(f"destination_uri: {destination_uri}")
        
        gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=destination_uri
        )

        # Where to write results
        output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

        # The full resource name of the processor, e.g.:
        # projects/project_id/locations/location/processor/processor_id
        # You must create new processors in the Cloud Console first
        name = client.processor_path(project_id, location, processor_id)

        request = documentai.BatchProcessRequest(
            name=name,
            input_documents=input_config,
            document_output_config=output_config,
        )

        # BatchProcess returns a Long Running Operation (LRO)
        operation = client.batch_process_documents(request)
        

        try:
            # Continually polls the operation until it is complete.
            # This could take some time for larger files
            # Format: projects/PROJECT_NUMBER/locations/LOCATION/operations/OPERATION_ID
            print(f"Waiting for operation {operation.operation.name} to complete...")
            operation.result(timeout=timeout)
        except:
            metadata : BatchProcessMetadata = documentai.BatchProcessMetadata(operation.metadata)
            print(f"Batch Process Failed: {metadata.state_message}")
            

        # NOTE: Can also use callbacks for asynchronous processing
        #
        # def my_callback(future):
        #   result = future.result()
        #
        # operation.add_done_callback(my_callback)

        # Once the operation is complete,
        # get output document information from operation metadata
        metadata : BatchProcessMetadata = documentai.BatchProcessMetadata(operation.metadata)
        
        # if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        #   print(f"Batch Process Failed: {metadata.state_message}")
        
            #raise ValueError(f"Batch Process Failed: {metadata.state_message}")

        return metadata
           
  

    def batch_process(  self,  project_id: str,
        location: str,
        processor_id: str,
        gcs_input_prefix: str,
        gcs_output_uri: str):

        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

        docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

        RESOURCE_NAME = docai_client.processor_path(project_id, location, processor_id)

        # Cloud Storage URI for the Input Directory
        gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_prefix)

        # Load GCS Input URI into Batch Input Config
        input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)

        # Cloud Storage URI for Output directory
        gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
            gcs_uri=gcs_output_uri
        )

        # Load GCS Output URI into OutputConfig object
        output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

        # Configure Process Request
        request = documentai.BatchProcessRequest(
            name=RESOURCE_NAME,
            input_documents=input_config,
            document_output_config=output_config,
        )

        # Batch Process returns a Long Running Operation (LRO)
        operation = docai_client.batch_process_documents(request)

        print(f"Waiting for operation {operation.operation.name} to complete...")
        operation.result()

        print("Document processing complete.")

                
    def process( self, **kwargs ):       
        print(f"clean_path : {self._clean_file_path}")
        print(f"file_prefix : {self._file_prefix}")
        print(f"sub_folder : {self._sub_folder}")
        print(f"bucket_name : {self._bucket_name}")
        print(f"file_name : {self._file_name}")
        print(f"raw_text_file_path : {self._raw_text_file_path}")
        print(f"hcls_nl_json_uri : {self._hcls_nl_json_uri}")
        print(f"creds : {self._creds}")
        print(f"project id : {self._project_id}")
        print(f"input_gcs_uri : {self._input_gcs_uri}")
        print(f"first_gcs_uri : {self._first_gcs_uri}")
        print(f"updated_timestamp_str : {self._updated_timestamp_str}")

        uri=self._input_gcs_uri
        from os.path import exists
        str_array = uri.split('/')
        folder = str_array[-1] 
        file_path = f"/content/drive/MyDrive/workspace/{folder}_metadata.pickle"
        

        self._batch_process_documents(self, "kallogjeri-project-345114", "us", "618bddd4359b5183","gs://medical_text_demo/pdf","gs://medical_text_demo/doc_ai_processed")

        # if self._input_gcs_uri.endsWith('.pdf'):
        #     self._batch_process( self._project_id
        #     , self._location
        #     , self._proc_id            
        #     , f"gs://{self._bucket_name}/pdf")                

        
        
        
        # if self._input_gcs_uri.endswith(".pdf"):
        #     if not exists(file_path):
                
        #         metadata = self.batch_process_new ( 
        #             project_id = self._project_id,
        #             location = self.location,
        #             processor_id = self._proc_id,
        #             gcs_input_prefix = self._file_prefix,
        #             gcs_output_uri = self._input_gcs_uri
        #         )                                        
                        
        #         str_array : list = self._input_gcs_uri.split('/')            
        #         folder = str_array[-1]     

        #         import pickle
        #         with open(file_path, 'wb') as f:
        #             pickle.dump(metadata, f)
        #     else:            
        #         metadata = None
        #         with open(file_path, 'rb') as handle:
        #             metadata = pickle.load(handle)
                
        #         if metadata is not None:
        #             print("Output files:")
        #             # One process per Input Document
        #             for process_status in metadata.individual_process_statuses:
        #                 print(process_status)
                    
        
        
        
                    


        



        
                    