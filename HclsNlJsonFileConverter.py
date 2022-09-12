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
            single_entity['first_gcs_path'] = first_gcs_path
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
 
    
    def process( self, **kwargs ):       
        print(f"clean_file_path : {self._clean_file_path}")
        print(f"file_prefix : {self._file_prefix}")
        print(f"sub_folder : {self._sub_folder}")
        
        res : dict = self._load_json_as_dict(self._content)       
        ents, mentions, rels = self._get_entities(res, self._first_gcs_uri, self._input_gcs_uri, self._updated_timestamp_str )    
       
    
        # text_annotations['text_segment_annotations'] =  get_text_annotations( res )
        # text_annotations['textContent'] = document.text
        # #print(text_annotations)
        # text_entities.append( text_annotations )   

        entity_file_name = f"entity_{self._file_prefix}.ndjson"        
        bq_entity_file_path = f"bq_import/{entity_file_name}"
        print(f"file_path : { bq_entity_file_path }")
        
                 
        upload_str_to_bucket(self.to_jsonl( list(ents.values())), bucket_name=self._bucket_name, file_path=bq_entity_file_path) 

        
                    