#from google.cloud.documentai_v1.types.document_processor_service import BatchProcessMetadata
import re

from google.api_core.client_options import ClientOptions
from google.cloud import documentai as documentai
from google.cloud import bigquery
import pickle

def concat(*args, sep=","):
    return sep.join(args)

def batch_process(    project_id: str,
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




def process_uris(
    uris: list
    , project_id: str
    , location: str 
    , proc_id : str 
    , bq_dataset : str 
    , workspace_home : str
  ):
  print(f"PROC_ID : {proc_id}")
  
  #metadata_list = list()
  from google.cloud import storage
  storage_client = storage.Client()  
  #uris = (uri for uri in uris if len(uri)>0)

  gcs_keys = dict()
  
  for uri in uris:
    
    if len(uri) > 0:    
      matches = re.match(r"gs://(.*?)/(.*)", uri)
      
      bucket_name = None
      folder_path = None
      if matches:    
        bucket_name, folder_path = matches.groups()



      print(f"uri : {uri}")
      print(f"Output bucket: {bucket_name}" )
      print(f"Output prefix: {folder_path}" )

      # Get List of Document Objects from the Output Bucket
      #bucket = storage_client.get_bucket(bucket_name)    
      #blobs = bucket.list_blobs(prefix="{folder_path}/")
      #blobs = storage_client.list_blobs(bucket_name, prefix='bq_import')       

      

      # bucket = storage.Client().get_bucket(bucket_name)
      blobs = list(storage_client.list_blobs(bucket_name, prefix=f"{folder_path}/", fields="items(name)"))
      
      raw_text_file_path = ''
      hcls_nl_json_uri = ''
      for blob in blobs:
        
        if not blob.name.endswith('/'):                       
          # Download JSON File as bytes object and convert to Document Object
          print(f"Fetching {blob.name}")
            
          file_path=blob.name
          arr = file_path.split('.')   
          
          split = arr[0].split('/')

          file_key = split[1].replace('document_','').replace('entity_', '')
          first_gcs_path = gcs_keys.get(file_key, f"gs://{bucket_name}/{blob.name}" )
          
          if first_gcs_path not in gcs_keys.keys():
            gcs_keys[file_key]=first_gcs_path  
          
          # if 'pdf' in blob.name or 'raw_text' in blob.name and not file_key in gcs_keys.keys():                             
            
          #   print(f"first_gcs_path: {first_gcs_path}")
                                        

          if 'raw_text' in blob.name:
            raw_text_file_path =  f"gs://{bucket_name}/{blob.name}"            
          elif 'hcls_nl_json' in blob.name:
            hcls_nl_json_uri = f"gs://{bucket_name}/{blob.name}"  
            
          folder = split[0]
          prefix = folder.capitalize()
          class_name = f"{prefix}FileConverter"

          from gcputils.PipelineRunner import PipelineRunner

          runner = PipelineRunner()      
          gcs_path=f"gs://{bucket_name}/{file_path}"
          module_name='gcputils'
          
          print(f"raw_text_file_path : {raw_text_file_path}")
          print(f"in docai, first_gcs_path : {first_gcs_path}")
          runner.process( module_name
          , class_name
          , project_id
          , gcs_path
          , first_gcs_path 
          , raw_text_file_path 
          , hcls_nl_json_uri
          , location
          , proc_id
          , bq_dataset
          , workspace_home )

                                    

if __name__ == '__main__':
  PROJECT_ID='kallogjeri-project-345114'
  BUCKET_NAME='medical_text_demo'
  
  # Format is 'us' or 'eu'
  LOCATION = 'us'   # @param {type:"string"} <---CHANGE THESE
  # Create processor in Cloud Console
  #PROC_ID = 'c136dd1d86619949' # @param {type:"string"} <---CHANGE THESE
  PROC_ID = '618bddd4359b5183' # @param {type:"string"} <---CHANGE THESE

  # Format: gs://bucket/directory/
  GCS_INPUT_URI = "gs://medical_text_demo/pdf"   
  GCS_INPUT_URI2 = "gs://medical_text_demo/raw_text"  # @param {type:"string"} <---CHANGE THESE  
  GCS_INPUT_URI3 = "gs://medical_text_demo/hcls_nl_json"  # @param {type:"string"} <---CHANGE THESE
  GCS_INPUT_URI4 = "gs://medical_text_demo/bq_import"  # @param {type:"string"} <---CHANGE THESE

  DATASET_NAME = 'entities'

  WORKSPACE_HOME = '/drive/MyDrive/workspace'

  from gcputils.doc_ai import process_uris

  gcs_input_str = concat(GCS_INPUT_URI, GCS_INPUT_URI2, GCS_INPUT_URI3, GCS_INPUT_URI4)
  gcs_inputs = gcs_input_str.split(',')

  from gcputils.doc_ai import process_uris
  from google.cloud import documentai_v1 as documentai  

  process_uris( 
      gcs_inputs
    , project_id=PROJECT_ID
    , location=LOCATION
    , proc_id=PROC_ID
    , bq_dataset=DATASET_NAME
    , workspace_home=WORKSPACE_HOME
  )                  

            

