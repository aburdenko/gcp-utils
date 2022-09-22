from gcputils.FileConverter import FileConverter
from gcputils.hcls_nlp import analyze_entities
from gcputils.cloud_storage import upload_str_to_bucket
from google.cloud.documentai_v1.types.document_processor_service import BatchProcessMetadata
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
from google.cloud import storage
import pickle

class PdfFileConverter(FileConverter):
    
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

   
    def _batch_process(  self,  project_id: str,
            location: str,
            processor_id: str,
            gcs_input_prefix: str,
            gcs_output_uri: str,
            timeout: int = 20000
        )->BatchProcessMetadata:

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
        
        metadata : BatchProcessMetadata = documentai.BatchProcessMetadata(operation.metadata)
                
        if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
            print(f"Batch Process Failed: {metadata.state}")                    
            raise ValueError(f"Batch Process Failed: {metadata.state_message}")

        return metadata
    
    def _extract_document_text(self, gcs_uri):
        storage_client = storage.Client()
            
        import re
        matches = re.match(r"gs://(.*?)/(.*)", gcs_uri)

        output_array = list()
            
        if matches:    
            output_bucket, output_prefix = matches.groups()

            #print(f"Output prefix: {output_prefix}" )

            # Get List of Document Objects from the Output Bucket
            output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)

            # Document AI may output multiple JSON files per source file
            for blob in output_blobs:
                
                # Document AI should only output JSON files to GCS
                if ".json" not in blob.name:
                    print(
                        f"Skipping non-supported file: {blob.name} - Mimetype: {blob.content_type}"
                    )
                    continue

                # Download JSON File as bytes object and convert to Document Object
                print(f"Fetching {blob.name}")
                document = documentai.Document.from_json(
                    #blob.download_as_bytes(ignore_unknown_fields=True)
                    blob.download_as_string()
                )

                output_array.append(document.text)
                
                                            
        return ' '.join(output_array)

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

        print(file_path)        

        gcs_output_bucket: str = f"gs://{self._bucket_name}"
        destination_uri = f"{gcs_output_bucket}/docai_processed"
        

        if not exists(file_path):
            
            metadata : BatchProcessMetadata = self._batch_process( self._project_id, self._location, self._proc_id, self._input_gcs_uri, destination_uri )     
                    
            str_array : list = self._input_gcs_uri.split('/')            
            folder = str_array[-1]     
            
            with open(file_path, 'wb') as f:
                pickle.dump(metadata, f)
        else:            
            metadata = None
            with open(file_path, 'rb') as handle:
                metadata = pickle.load(handle)

        if metadata is not None:
            print("Output files:")
            # One process per Input Document
            for process_status in metadata.individual_process_statuses:
                print(process_status)

                text = self._extract_document_text(process_status.output_gcs_destination)                                        
                self._raw_text_file_path =  f"raw_text/{self._file_name.replace('.pdf', '.txt')}"
                upload_str_to_bucket(text, bucket_name=self._bucket_name, file_path=self._raw_text_file_path)

        

        

        



        
                    