from gcputils.FileConverter import FileConverter
from gcputils.hcls_nlp import analyze_entities
from gcputils.cloud_storage import upload_str_to_bucket

class RawTextFileConverter(FileConverter):
    
    raw_text_file_path=None
    
       
    def process( self, **kwargs ):       
        print(f"clean_path : {self._clean_file_path}")
        print(f"file_prefix : {self._file_prefix}")
        print(f"sub_folder : {self._sub_folder}")
        print(f"bucket_name : {self._bucket_name}")
        print(f"file_name : {self._file_name}")
        print(f"raw_text_file_path : {self._raw_text_file_path}")
        print(f"hcls_nl_json_uri : {self._hcls_nl_json_uri}")

        if not RawTextFileConverter.raw_text_file_path:
            RawTextFileConverter.raw_text_file_path = f"raw_text/{self._file_prefix}.txt"
    

        
        str_item=self._content
        creds=self._creds
        project=self._project_id
        raw_entities = list()
        res = analyze_entities( str_item=str_item, creds=creds, project=project )
        raw_entities.append(res)
        self._output_path = f"hcls_nl_json/{self._file_prefix}.json"
        upload_str_to_bucket(self._to_jsonl(raw_entities), bucket_name=self._bucket_name, file_path=self._output_path) 
        




        
                    