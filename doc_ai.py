from google.cloud.documentai_v1.types.document_processor_service import BatchProcessMetadata
import re

from google.api_core.client_options import ClientOptions
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.cloud import bigquery
import pickle


# TODO(developer): Uncomment these variables before running the sample.
PROJECT_ID = '419103448378'  # @param {type:"string"} <---CHANGE THESE
# Format is 'us' or 'eu'
LOCATION = 'us'   # @param {type:"string"} <---CHANGE THESE
# Create processor in Cloud Console
#PROC_ID = 'c136dd1d86619949' # @param {type:"string"} <---CHANGE THESE
PROC_ID = 'bbe9f47188b1fc33' # @param {type:"string"} <---CHANGE THESE

#GCS_INPUT_URI = "gs://ocr_e2e_input/gene.pdf" # Format: gs://bucket/directory/file.pdf

# Format: gs://bucket/directory/
GCS_INPUT_URI = "gs://medical_text_demo/incoming"  # @param {type:"string"} <---CHANGE THESE
GCS_INPUT_URI2 = "gs://medical_text_demo/hcls_nl_json"  # @param {type:"string"} <---CHANGE THESE
GCS_INPUT_URI3 = "gs://medical_text_demo/bq_import"  # @param {type:"string"} <---CHANGE THESE



def concat(*args, sep=","):
    return sep.join(args)

gcs_input_str = concat(GCS_INPUT_URI, GCS_INPUT_URI2, GCS_INPUT_URI3)
gcs_inputs = gcs_input_str.split(',')

INPUT_MIME_TYPE = "application/pdf"
#GCS_OUTPUT_BUCKET = "gs://splitterout" # Format: gs://bucket
GCS_OUTPUT_BUCKET = "cai-poc-dataset"  # @param {type:"string"} <---CHANGE THESE
# Format: directory/subdirectory
GCS_OUTPUT_PREFIX = "doc_ai_processed"   # @param {type:"string"} <---CHANGE THESE


def batch_process_documents(
    project_id: str = PROJECT_ID,
    location: str = LOCATION,
    processor_id: str = PROC_ID,
    gcs_input_uri: str = GCS_INPUT_URI,
    input_mime_type: str = INPUT_MIME_TYPE,
    gcs_output_bucket: str = f"gs://{GCS_OUTPUT_BUCKET}",
    gcs_output_uri_prefix: str = GCS_OUTPUT_PREFIX,
    timeout: int = 20000,
)->BatchProcessMetadata:

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
    destination_uri = f"{gcs_output_bucket}/{gcs_output_uri_prefix}/"
    print(destination_uri)
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

def get_text_annotations(resp)->list:
  #print(resp)  
  entity_keys = list()
  automl_entities = list()
  for entity_mention in resp.get('entityMentions', []):
    #print(entity_mention)
    
    #linked_entity_list = entity_mention.get('linkedEntities', None)
    
    #if not linked_entity_list is None:
    #  for linked_entity in linked_entity_list:        
          

    entity_text = entity_mention['text'].get('content', '') # Insulin regimen human          
    entity_type = entity_mention['type']
    
    start_offset = entity_mention['text'].get('beginOffset', -1)
    
    ln = len(entity_text)  
    end_offset = start_offset + ln                    
    
    #highlight_text = raw_text[start_offset:end_offset]
    
    #print(f"highlight_text: {highlight_text}")
    # print(f"entity_type: {entity_type}")
    # print(f"entity_text: {entity_text}")

    if (start_offset, end_offset) not in entity_keys: 
      
        #automl_entities.append( automl_entity )
      if start_offset > 0 :
        automl_entity=dict()          
        automl_entity['endOffset']=end_offset
        automl_entity['startOffset']=start_offset      
        automl_entity['displayName']=entity_type
        #print(automl_entity)
        #print(len(automl_entity))
        automl_entities.append( automl_entity )                  
        entity_keys.append( (start_offset, end_offset) )
              


    return automl_entities
def to_jsonl( entity ):
  import json
  from io import StringIO
  #result = [json.dumps(entity) for entity in entities]
  #result = [json.dumps(list(entity.values())) for entity in entities]
  res = [json.dumps(record) for record in entity]    
  buf = StringIO()
  for i in res:
      buf.write(i+'\n')
  
  sbuf = buf.getvalue().replace('"-','\"-')
  sbuf = buf.getvalue().replace("'-","-")
  sbuf = buf.getvalue().replace("'","\"")
  sbuf = buf.getvalue().replace("#","\#")
  

  #print(f"sbuf is {sbuf}")
  # for i in res:
  #   lines = i.split('},')
  #   sbuf = '\n'.join( lines )
  #   sbuf = sbuf.replace("'","\"")
  #   print(sbuf )
  #   buf.write( sbuf )

  # for i in res:
  #   lines = i.split('},')
  #   buf.write('\n'.join( lines ) )
  
  return sbuf


def get_document(gcs_path : str, raw_text : str, was_ocrd : bool, automl_dataset_id : str)->dict:  
    
  docs = dict()       
  single_doc = dict()     
  #single_doc['gcs_uri'], _, _ = get_clean_path(gcs_path)
  single_doc['gcs_uri'] = gcs_path
  single_doc['raw_text'] = raw_text     
  single_doc['was_ocrd'] = True       
  single_doc['was_abstracted'] = True
  single_doc['automl_dataset_url'] = f"https://console.cloud.google.com/vertex-ai/locations/us-central1/datasets/{automl_dataset_id}/browse?project=qp-ml-demoresearch-2021-05"
  docs[gcs_path] = single_doc
  
  return docs  

from google.cloud.aiplatform.datasets.text_dataset import TextDataset
from google.cloud import aiplatform as aip
def create_aip_dataset(display_name, source : list, export_bucket_name : str, existing_ds_name : str = None ): 
  from datetime import datetime
  import google.cloud.aiplatform as aip
  
  TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")

  existing_dataset = None
  if existing_ds_name is not None:        
    file_path=f"gs://{export_bucket_name}/automl_dataset/{existing_ds_name}"    
    existing_dataset = aip.TextDataset(dataset_name=existing_ds_name)
    existing_dataset.export_data( file_path )
  
  dataset : TextDataset = aip.TextDataset.create(
      display_name=display_name + "_" + TIMESTAMP,
      gcs_source=source,
      import_schema_uri=aip.schema.dataset.ioformat.text.extraction
  )

  if existing_dataset is not None:       
    dataset.import_data( file_path, import_schema_uri=aip.schema.dataset.ioformat.text.extraction )

  print(dataset.name)

  return dataset.name
  

  #print(dataset.resource_name)  
from google.cloud.bigquery.enums import WriteDisposition
def process_document( process_status, gcs_file_name, document, text_annotations : dict, write_disposition : WriteDisposition = bigquery.WriteDisposition().WRITE_TRUNCATE): 
  # For a full list of Document object attributes, please reference this page:
  # https://cloud.google.com/python/docs/reference/documentai/latest/google.cloud.documentai_v1.types.Document

  # Read the text recognition output from the processor
  # print("The document contains the following text:")
  # print(document)
  # print(document.text)
    #gs://concertai-poc-dataset/ConcertAI Dataset/GeneticTesting_NGSdocs/Caris_12345678A.pdf



  clean_path, file_prefix, sub_folder = get_clean_path( process_status.input_gcs_source )
   
  print(f"clean_path : {clean_path}")
  print(f"file_prefix : {file_prefix}")
  print(f"sub_folder : {sub_folder}")

  file_name = f"{file_prefix}.txt"      
  folder_path = f"raw_text/{folder}"
  raw_text_file_path = f"{folder_path}/{file_name}"
  

  file_name = f"{file_prefix}.json"      
  folder_path = f"nlp_processed/{folder}"
  nlp_file_path = f"{folder_path}/{file_name}"

  file_name = f"{file_prefix}.jsonl"
  automl_folder_path = f"automl_dataset/{folder}"
  automl_file_path = f"{automl_folder_path}/{file_name}"

  doc_file_name = f"{file_prefix}_doc.ndjson"
  file_name = f"{file_prefix}.ndjson"
  folder_path = f"bq_import/{folder}"
  print(f"folder_path : {folder_path}")
  
  file_name=sanitize_input_path(file_name)  
  doc_file_name=sanitize_input_path(doc_file_name)  

  bq_entity_file_path = f"{folder_path}/entity_{doc_file_name}"  
  #bq_entity_file_path = f"test/entity_{doc_file_name}"  
  bq_rel_file_path = f"{folder_path}{sub_folder}/relationship_{doc_file_name}"  
  #bq_rel_file_path = f"gs://{BUCKET_NAME}/{bq_rel_file_path}"
  bq_doc_file_path = f"{folder_path}{sub_folder}/document_{doc_file_name}"
  #bq_doc_file_path = f"gs://{BUCKET_NAME}/{bq_doc_file_path}"

  print(f"bq_doc_file_path : {bq_doc_file_path}")
  print(f"bq_rel_file_path : {bq_rel_file_path}")
  
              
  # call HCLS NLP API and get the raw rs
  import time
  start_time = time.time()      
  print(document.text )    

  from hcls_nlp import analyze_entities
  PROJECT_ID='qp-ml-demoresearch-2021-05'
  
  GOOGLE_APPLICATION_CREDENTIALS = '/content/service_account.json'
  creds = get_credentials(GOOGLE_APPLICATION_CREDENTIALS) 
  res = analyze_entities( document.text, project=PROJECT_ID, creds=creds )                   
  end_time = time.time()

  from datetime import datetime
  timestamp = str(datetime.now())
  # end 

  raw_file_path, nlp_file_path = process_status.input_gcs_source, gcs_file_name
  print(raw_file_path) 
  raw_entities.append( res )  
  print(res)
  entities =  get_entities(res, document.text, raw_file_path, nlp_file_path, automl_file_path)                 
  #entities.append( ents )
  
  #rels.append(  get_relationships( res, mentions, clean_path, timestamp ) )
  
  

  text_annotations['text_segment_annotations'] =  get_text_annotations( res )
  text_annotations['textContent'] = document.text
  #print(text_annotations)
  text_entities.append( text_annotations )   

  #write jsonl for automl import to GCS            
  [upload_str_to_bucket(to_jsonl(text_entities), bucket_name=GCS_OUTPUT_BUCKET, file_path=automl_file_path) for entity in entities if entity is not None and len(entity)>0]

  full_path=f"gs://{BUCKET_NAME}/{automl_file_path}"
  automl_dataset_id = create_aip_dataset( display_name=gcs_file_name, export_bucket_name=None, source=[full_path] )
  docs.append( get_document( f"{bq_doc_file_path}", document.text, True, automl_dataset_id ) )
  [upload_str_to_bucket( to_jsonl( list( doc.values()) ), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_doc_file_path) for doc in docs if doc is not None and len(doc)>0]         

  
  

  #for entity in list(entities.values()):    
  #upload_str_to_bucket( to_jsonl(list(entities.values())) , bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path)  

  # Write raw document text to gcs bucket
  #upload_str_to_bucket(document.text, bucket_name=BUCKET_NAME, file_path=raw_text_file_path )
        
  # write ndjson for entities bq import to GCS bucket
  #print(entities)
  #[upload_str_to_bucket(list(entity.values()), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path) for entity in entities if entity is not None and len(entity)>0 and type(entity) == 'dict' ]         
  #[upload_str_to_bucket( to_jsonl( list(entity.values()) ), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path ) for entity in entities if entity is not None and len(entity)>0 and type(entity) == 'dict']
#  [upload_str_to_bucket(to_jsonl(list(entity.values())), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path) for entity in entities if entity is not None and len(entity)>0]      
  #upload_str_to_bucket(  to_jsonl(list(entities[0].values())), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path ) 
  #[upload_str_to_bucket(to_jsonl(list(entity.values())), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path) for entity in entities if entity is not None and len(entity)>0]         

  # write ndjson for relationships bq import to GCS bucket  

  # print(f"uploading {bq_rel_file_path}")
  # print(rels)
  # [upload_str_to_bucket( to_jsonl( list( rel.values()) ), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_rel_file_path) for rel in rels if rel is not None and len(rel)>0]         
  #bq_import( 'entities', 'Relationship', bq_rel_file_path, src_fmt=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=write_disposition  ) 

  
  
#   full_path=f"gs://{BUCKET_NAME}/{automl_file_path}"

#   automl_dataset_id = create_aip_dataset( display_name=gcs_file_name, export_bucket_name=None, source=[full_path] )
#   # if full_path not in aip_dataset_file_list:
#   #   aip_dataset_file_list.append( full_path )
  
    

#   # #Create the Vertex AI Dataset import files
#   # [upload_str_to_bucket(to_jsonl( list(entity.values()) ), bucket_name=GCS_OUTPUT_BUCKET, file_path=bq_entity_file_path) for entity in entities if entity is not None and len(entity)>0]
 


#   # write ndjson for docs bq import to GCS bucket
    
  
#   print(f"uploading {bq_doc_file_path}")
#bq_import( 'entities', 'Document', bq_doc_file_path, src_fmt=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=write_disposition  )     

# #print(metadata)
# raw_entities = list()
# entities = list()
# docs = list()
# rels = list()

# text_entities = list()
# #df = DataFrame(columns=["STATUS", "FILE_PATH"])
# folder: str = ""
# aip_dataset_file_list = list()

raw_entities = list()
entities = list()
docs = list()
rels = list()
text_entities = list()

def nlp_process_documents(gcs_uri):
  storage_client = storage.Client()
    
  
  matches = re.match(r"gs://(.*?)/(.*)", gcs_uri)
    
  if matches:    
    output_bucket, output_prefix = matches.groups()

    #print(f"Output prefix: {output_prefix}" )

    # Get List of Document Objects from the Output Bucket
    output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)

    # Document AI may output multiple JSON files per source file


    for blob in output_blobs:
      text_annotations = dict()

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
        
                      
      #if not already_processed( process_status.input_gcs_source ):                                        
      process_document( process_status, blob.name, document, text_annotations, write_disposition )
    #return

def _create_instance(self, module_name, class_name, attrsMap: dict):                                       
        import importlib                        
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)                

        instance = class_(attrsMap)         

        
            
        return instance


def process_pdf( uri: str ) :
  print(f"processing uri: {uri}...")  

  from os.path import exists
  str_array = uri.split('/')
  folder = str_array[-1] 
  file_path = f"/content/drive/MyDrive/workspace/{folder}_metadata.pickle"


  if not exists(file_path):
      metadata = batch_process_documents(gcs_input_uri=uri)
      metadata_list.append(metadata)
      
      str_array = uri.split('/')
      folder = str_array[-1]     

      import pickle
      with open(file_path, 'wb') as f:
          pickle.dump(metadata, f)
  else:
      if ('Additional_Redacted_Patient_Document_metadata.pickle' in file_path):
          metadata = None
          with open(file_path, 'rb') as handle:
              metadata = pickle.load(handle)
          
          if metadata is not None:
            print("Output files:")
            # One process per Input Document
            for process_status in metadata.individual_process_statuses:
              #print(metadata)
              # output_gcs_destination format: gs://BUCKET/PREFIX/OPERATION_NUMBER/INPUT_FILE_NUMBER/
              # The Cloud Storage API requires the bucket name and URI prefix separately              
              nlp_process_documents(process_status.output_gcs_destination)      
      
def process_uris(uris: list, project_id: str):
  metadata_list = list()
  from google.cloud import storage
  storage_client = storage.Client()  
  uris = (uri for uri in gcs_inputs if len(uri)>0)
  
  for uri in uris:
    
    matches = re.match(r"gs://(.*?)/(.*)", uri)

    
    
    if matches:    
      bucket_name, folder_path = matches.groups()

    print(f"Output bucket: {bucket_name}" )
    print(f"Output prefix: {folder_path}" )

    # Get List of Document Objects from the Output Bucket
    #bucket = storage_client.get_bucket(bucket_name)    
    #blobs = bucket.list_blobs(prefix="{folder_path}/")
    #blobs = storage_client.list_blobs(bucket_name, prefix='bq_import')       

    

    bucket = storage.Client().get_bucket(bucket_name)
    blobs = list(storage_client.list_blobs(bucket_name, prefix=f"{folder_path}/", fields="items(name)"))
    
    is_first = True
    for blob in blobs:
      
      if not blob.name.endswith('/'):

        print(str(blob))

        if is_first:
          first_gcs_path = blob.name
          is_first = False


        # Download JSON File as bytes object and convert to Document Object
        print(f"Fetching {blob.name}")
          
        file_path=blob.name
        arr = file_path.split('.')   
        
        folder = arr[0].split('/')[0]
        prefix = folder.capitalize()
        class_name = f"{prefix}FileConverter"

        from gcputils.PipelineRunner import PipelineRunner

        runner = PipelineRunner()      
        gcs_path=f"gs://{bucket_name}/{file_path}"
        module_name='gcputils'
        
        runner.process( module_name, class_name, project_id, gcs_path, first_gcs_path )

        
                                    

if __name__ == '__main__':
  PROJECT_ID='kallogjeri-project-345114'
  BUCKET_NAME='medical_text_demo'
  
  process_uris(gcs_inputs, project_id=PROJECT_ID)          

            

