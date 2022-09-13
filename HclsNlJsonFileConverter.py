from gcputils.FileConverter import FileConverter
from gcputils.cloud_storage import upload_str_to_bucket

class HclsNlJsonFileConverter(FileConverter):


    def _get_entities( self, resp : dict, first_gcs_path : str, input_gcs_uri : str, updated_timestamp: str ):  
        import json
        entities = dict()     
        entity_mentions=list()
        relations=list()
        mentions = list()
        

        for entity in resp.get('entities', []):
            single_entity = dict()     
        
            single_entity['entity_id'] =  entity['entityId']
            single_entity['preferred_term'] = entity.get('preferredTerm', None)
            
            single_entity['vocabulary_codes'] = entity['vocabularyCodes']
            single_entity['first_gcs_uri'] = first_gcs_path
            single_entity['input_gcs_uri'] = input_gcs_uri            
            single_entity['updated_timestamp'] = updated_timestamp            
                            
            entities[single_entity['entity_id']] = single_entity

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
        

            linked_entity_list = entity_mention.get('linkedEntities', None)
            if not linked_entity_list is None:
                for linked_entity in linked_entity_list:
            
                    #single_entity.update( entities.get( entity['entityId'] ) )                        
                            
                    entity_id = linked_entity['entityId']         
                    entity = entities.get( entity_id,  None)
                    if entity is not None:
                        mentions = entity.get('mentions', None)
                        if mentions is None:
                            entity['mentions']  = list()
          
            simple_mention['entity_id']=entity['entity_id']
            entity['mentions'].append( simple_mention )        
            entities[ entity['entity_id'] ]  = entity

            entity_mentions.append(simple_mention)
        
        i = i + 1

        for rel in resp.get('relationships', []): 
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
                    
        return entities, mentions, relations

    def _load_json_as_dict( self, json_text  )->dict:
        # importing the module
        import json 
        data : dict = json.loads(json_text)

        return data

    def _get_text_annotations( self, resp )->list:
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
 
    def _get_document(self, input_gcs_uri : str, first_gcs_uri : str, was_ocrd : bool, automl_dataset_id : str)->dict:  
    
        docs = dict()       
        single_doc = dict()     
        #single_doc['gcs_uri'], _, _ = get_clean_path(gcs_path)
        single_doc['input_gcs_uri'] = input_gcs_uri        
        single_doc['first_gcs_uri'] = first_gcs_uri        
        single_doc['was_ocrd'] = True       
        single_doc['was_abstracted'] = True
        single_doc['automl_dataset_url'] = f"https://console.cloud.google.com/vertex-ai/locations/us-central1/datasets/{automl_dataset_id}"
        docs[input_gcs_uri] = single_doc
        
        return docs  

        from google.cloud.aiplatform.datasets.text_dataset import TextDataset
        from google.cloud import aiplatform as aip
    
    def _create_aip_dataset(self, display_name, source : list, export_bucket_name : str, existing_ds_name : str = None ): 
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
  

    def process( self, **kwargs ):                       
        res : dict = self._load_json_as_dict(self._content)       
                   
        entity_file_name = f"entity_{self._file_prefix}.ndjson"        
        bq_entity_file_path = f"bq_import/{entity_file_name}"
        print(f"file_path : { bq_entity_file_path }")

        doc_file_name = f"document_{self._file_prefix}.ndjson"        
        bq_doc_file_path =  f"bq_import/{doc_file_name}"        

        ents, mentions, rels = self._get_entities(res, self._first_gcs_uri, f"gs://{self._bucket_name}/{bq_entity_file_path}", self._updated_timestamp_str )    
                 
        upload_str_to_bucket(self._to_jsonl( list(ents.values())), bucket_name=self._bucket_name, file_path=bq_entity_file_path) 

        # Create Vertex AI dataset of it doesn't already exist
        text_entities = list()
        text_annotations = dict()
        text_annotations['text_segment_annotations'] =  self._get_text_annotations( res )
        text_annotations['textContent'] = self._content
        text_entities.append( text_annotations )   

        was_ocrd : bool = True if 'pdf' in self._input_gcs_uri else False

        doc = self._get_document(f"gs://{self._bucket_name}/{bq_doc_file_path}", self._first_gcs_uri,  was_ocrd, None)        
        upload_str_to_bucket( self._to_jsonl( list( doc.values()) ), bucket_name=self._bucket_name, file_path=bq_doc_file_path)    

        

        
                    