from google.cloud import language_v1 as language
#from google.cloud import language

def get_one_mb_str_blocks( str_item: str, blocks: list ):
  #MAX_LEN=1000000  
  MAX_LEN=20000
  str_len = len( str_item.encode('utf-16') )
  
  if str_len > MAX_LEN :
     rest = str_item[MAX_LEN+1:]

     if len(rest.encode('utf-16')) > MAX_LEN:     
        get_one_mb_str_blocks( rest, blocks )
     else:
        blocks.append( rest ) 
     
   
  else:
      blocks.append( str_item ) 

from oauth2client.client import Credentials, Storage
import google.auth
def get_credentials(service_account_file=None)->Credentials:
    """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
        Credentials, the obtained credential.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    APPLICATION_NAME = "seres kg"

    from os import path, makedirs
    home_dir = path.expanduser("~")
    credential_dir = path.join(home_dir, ".credentials")
    client_secret_path = path.join(credential_dir,'creds.json')            
    if not path.exists(credential_dir): 
        makedirs(credential_dir)        
        
    store = Storage(client_secret_path)                
    creds : Credentials = None

    # try:
    #     store.get()
    # except Exception:
    #     pass

    
    if not creds:

        # See https://cloud.google.com/docs/authentication/getting-started
        
        creds, _ = google.auth.default() 

        if service_account_file:
                
            from google.oauth2 import service_account
            service_account_file_path = path.join(credential_dir, service_account_file)       
            creds = service_account.Credentials.from_service_account_file
            (
                service_account_file_path
                ,["https://www.googleapis.com/auth/cloud-platform"]
            )     
                                                                    
        if not creds:                                                                                       
                from oauth2client import client, tools
                flags = tools.argparser.parse_args(args=['--noauth_local_webserver'])                         
                flow = client.flow_from_clientsecrets(client_secret_path, SCOPES)
                flow.user_agent = APPLICATION_NAME
                creds = tools.run_flow(flow, store, flags)
    
    return creds

from google.cloud import _http
class Connection(_http.JSONConnection):
  """Handles HTTP requests to GCP."""
  API_BASE_URL = 'https://healthcare.googleapis.com'
  API_VERSION = 'v1beta1'
  #API_VERSION = 'v1'
  API_URL_TEMPLATE = '{api_base_url}/{api_version}/projects{path}'

from google.cloud.client import ClientWithProject
class Client(ClientWithProject):
  """A client for accessing Cloud Healthcare NLP API.

  Args:
      project (Union[str, None]): The ID of the project
      region (str): The region the project resides in, e.g. us-central1,
  """

  def __init__(self,
               project,
               region='us-central1',
               credentials=None,
               http=None):
    self.region = region
    self.SCOPE = ('https://www.googleapis.com/auth/cloud-healthcare',)
    super(Client, self).__init__(project=project)
    self.path = '/{}/locations/{}/services/nlp'.format(self.project,
                                                       self.region)
    # self._credentials = credentials       
    self.project=project     
    self._connection = Connection(self)    
        

  def analyze_entities(self, document):
    """ Analyze the clinical entities a document with the Google Cloud

      Healthcare NLP API.

      Args:
        document (str): the medical document to analyze.

      Returns:
        Dict[str, Any]: the JSON response.
      """
    
    # client = language.LanguageServiceClient()    
    # document = language.Document(content=document, type_=language.Document.Type.PLAIN_TEXT)


    # #document = language.Document(content=document, type = language.Document.Type.PLAIN_TEXT)
    # #document = language.Document(content=document, type_=language.Document.Type.PLAIN_TEXT)

    # client = language.LanguageServiceClient()
    # response = client.analyze_entities(document=document)


    # print( document )
    document = str( document.encode('utf-8') )
    
    return self._connection.api_request(
        'POST',
        self.path + ':analyzeEntities',
        data={'document_content': document})
        

    
def analyze_entities(str_item, project, creds)->dict:
   #API Limits the size of the text field to 1 MB
  str_blocks = list()          
  #str_item = str_item.encode(encoding='utf-8')
  get_one_mb_str_blocks( str_item, str_blocks )

  client = Client( project=project, credentials=creds )
  if len(str_blocks) > 1:
    last_fragment = str_blocks[len(str_blocks)-1]

  res = dict()
    
  for block in str_blocks:          

    # str_item = "Sepsis results in unfettered inflammation, tissue damage, and multiple organ failure. Diffuse brain dysfunction and neurological manifestations secondary to sepsis are termed sepsis-associated encephalopathy (SAE). Extracellular nucleotides, proinflammatory cytokines, and oxidative stress reactions are associated with delirium and brain injury, and might be linked to the pathophysiology of SAE. P2X7 receptor activation by extracellular ATP leads to maturation and release of IL-1β by immune cells, which stimulates the production of oxygen reactive species. Hence, we sought to investigate the role of purinergic signaling by P2X7 in a model of sepsis. We also determined how this process is regulated by the ectonucleotidase CD39, a scavenger of extracellular nucleotides. Wild type (WT), P2X7 receptor (P2X7)"        
    res.update( client.analyze_entities( block ) )   
          
  return res    

def get_entities(resp, text, raw_file_path, nlp_file_path, automl_file_path = None):  
  import json
  entities = dict()     
 

  for entity in resp.get('entities', []):
    single_entity = dict()     
   
    single_entity['entity_id'] =  entity['entityId']
    single_entity['preferred_term'] = entity.get('preferredTerm', None)
    
    single_entity['vocabulary_codes'] = entity['vocabularyCodes']
    single_entity['raw_file_path'] = raw_file_path
    single_entity['nlp_file_path'] = nlp_file_path
    single_entity['automl_file_path'] = automl_file_path    
    single_entity['text'] = text        
    if 'GeneticTesting_NGSdocs' in raw_file_path:
      single_entity['doc_type'] = 'Genetic Tests'  
    elif 'Lab reports_Redacted' in raw_file_path:
      single_entity['doc_type'] = 'Lab Reports' 
    
    
    entities[single_entity['entity_id']] = single_entity

  entity_mentions=list()
  i = 0
  for entity_mention in resp.get('entityMentions', []): 
    simple_mention = dict()   

    simple_mention['mention_id']=i
    simple_mention['confidence'] = entity_mention['confidence']    
    if entity_mention.get( 'subject', None ):
      simple_mention['subject'] = entity_mention['subject'].get('value', '') # Insulin regimen human

    simple_mention['type'] = entity_mention['type']
    start_offset = entity_mention['text'].get('beginOffset', 0)
    simple_mention['start_offset'] = start_offset
    entity_text = entity_mention['text'].get('content', '') # Insulin regimen human
    simple_mention['text'] = entity_text
    ln = len(entity_text)  
    end_offset = start_offset + ln
    simple_mention['end_offset'] = str(end_offset)

    # linked_entity_list = entity_mention.get('linkedEntities', None)
    # if not linked_entity_list is None:
    #   for linked_entity in linked_entity_list:
        
    #     #single_entity.update( entities.get( entity['entityId'] ) )                        
                          
    #     entity_id = linked_entity['entityId']         
    #     entity = entities.get( entity_id,  None)
    #     if entity is not None:
    #       mentions = entity.get('mentions', None)
    #       if mentions is None:
    #         entity['mentions']  = list()
          
    #       simple_mention['entity_id']=entity['entity_id']
    #       entity['mentions'].append( simple_mention )        
    #       entities[ entity['entity_id'] ]  = entity
    
    entity_mentions.append(simple_mention)     
    i = i + 1
  
    relations=list()
    for rel in resp.get("relationships", []): 
      subject_id = rel['subjectId']
      object_id = rel['objectId']

      subject_entity_mentions = [entity_mention for entity_mention in entity_mentions if entity_mention['mention_id'] == subject_id ]
      object_entity_mentions = [entity_mention for entity_mention in entity_mentions if entity_mention['mention_id'] == object_id ]

      if len(subject_entity_mentions)>0 and  len(object_entity_mentions)>0:
        object_entity_mention = object_entity_mentions[0]
        subject_entity_mention = subject_entity_mentions[0]
        object_entity_mention['relations'].append(subject_entity_mention)

        entity : dict = entities[ object_entity_mention['entity_id'] ]
        entity_mention : dict = [entity_mention for entity_mention in entity['mentions'] if entity['entity_id'] == object_entity_mention['entity_id'] ]
        
        if entity_mention.get( 'relations', None ) is None:
          entity_mention['relations'] = list()
        
        entity_mention['relations'] = object_entity_mention['relations']
        entity['entity_mentions'][int(entity_mention['mention_id'])] = entity_mention
        entities[ entity['entity_id'] ]  = entity
                    
  return entities, entity_mentions, relations

if __name__ == '__main__':
  PROJECT_ID='qp-ml-demoresearch-2021-05'
  import os
  GOOGLE_APPLICATION_CREDENTIALS = '/content/service_account.json'
  creds = get_credentials(GOOGLE_APPLICATION_CREDENTIALS) 
  
  str_item="VECTOR \
  ONCOLOGY \
  CONFIDENTIAL \
  PHYSICIAN \
  Dr. Lewis \
  Test Performed \
  BRCA1 sequencing \
  PATIENT \
  BRCA2 sequending \
  comprehensive rearrangement \
  COPY \
  Comprehensive BRACAnalysis \
  BRCA1 and BRCA2 Analysis Result \
  comprehensive rearrangement \
  SPECIMEN \
  Specimen Type: \
  Draw Date: \
  Accession Date: \
  Report Date: \
  Buccal Wash \
  Apr 18, \
  Apr 19, \
  2012 \
  May 02, \
  Test Results and Interpretation \
  POSITIVE FOR A DELETERIOUS MUTATION \
  Result \
  No Mutation Detected \
  No Mutation Detected \
  Name: \
  Date of Birth: \
  Patient ID: \
  Gender: \
  Accession #: \
  Requisition #: \
  Y3098X (9522C>G) \
  No Mutation Detected \
  MYRIAD. \
  PATIENT \
  Sally Owens \
  Interpretation \
  No Mutation Delected \
  No Mutation Detected \
  Deleterious \
  No Mutation Detected \
  It is our understanding that this patient was identified for testing due to a personal or family history suggestive of hereditary breast and \
  ovarian cancer. Analyals conalsts of sequending of all translated exons and immediately adjacent Intronic regions of the BRCA1 and \
  BRCA2 genes, a test for five specific BRCA1 rearrangements, and a comprehensive rearrangement test of both BRCA1 and BRCA2 by \
  quantitative PCR analysis (BRACAnalysis Rearrangement Test, BART). The classification and interpretation of all variants identified in \
  this assay reflects the current state of scientific understanding at the time this report was issued. In some instances, the classification and \
  interpretation of such variants may change as new scientific Information becomes available. \
  The results of this analysis are consistent with the germline BRCA2 mutation Y3098x, resulting in premature truncation of the BRCA2 \
  protein at amino acid position 3098. Although the exact risk of breast and ovarian cancer conferred by this specific mutation has not been \
  determined, studies of this type of mutation in high-risk families indicate that deleterious mutations in BRCA2 may confer as much as \
  an 84% risk of breast cancer and a 27% risk of ovarian cancer by age 70 in women (Am. J. Hum. Genet. 62:876-689, 1998). Mutations \
  in BRCA2 have been reported to confer a 12% risk of a second breast cancer within five years of the first (J Clin Oncol 17:3396-3402, \
  1998), as well as a 16% risk of subsequent ovarian cancer (J Natl Cancer Inst 91:1310-1315, 1999). Additionally, studies have shown \
  that BRCA2 mutations confer as much as a 7% risk of pancreatic cancer by age 80 (J Med Genet 42:711-8, 2005); however, this risk may \
  be higher in families in which pancreatic cancer has previously been diagnosed. This mutation may also confer up to an 8% risk of male \
  breast cancer and 20% risk of prostate cancer by age 80 (J Natl Cancer Inst 99:1811-4, 2007; J Natl Cancer Inst 91:1310-1315, 1999), as \
  well as increased (albeit low) risks of some other cancers. Each first degree relative of this individual has a one-in-two chance of having \
  this mutation. Family members can be teated for this specific mutation with a single site analysis." 
  text = "None"
  raw_file_path = 'gs://cai-poc-dataset/ConcertAI Dataset/GeneticTesting_NGSdocs/Caris_12345678A.pdf' 
  nlp_file_path = None
  automl_file_path = None
  resp = analyze_entities(str_item, PROJECT_ID, creds)
  entities, entity_mentions, relations = get_entities(resp, text, raw_file_path, nlp_file_path, automl_file_path = None)
  print(f"entities: {entities}")
  print(f"\n\mentions: {entity_mentions}")
  print(f"\n\relations: {relations}")