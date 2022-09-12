from gcputils.FileConverter import FileConverter
from gcputils.hcls_nlp import analyze_entities
from gcputils.cloud_storage import upload_str_to_bucket

class IncomingFileConverter(FileConverter):
    
       
    def process( self, **kwargs ):       
        print(f"clean_path : {self._clean_file_path}")
        print(f"file_prefix : {self._file_prefix}")
        print(f"sub_folder : {self._sub_folder}")
        print(f"bucket_name : {self._bucket_name}")
        print(f"file_name : {self._file_name}")


        str_item=self._content
        creds=self._creds
        project=self._project_id
        raw_entities = list()
        res = analyze_entities( str_item=str_item, creds=creds, project=project )
        raw_entities.append(res)
        self._output_path = f"hcls_nl_json/{self._file_prefix}.json"
        upload_str_to_bucket(self.to_jsonl(raw_entities), bucket_name=self._bucket_name, file_path=self._output_path) 
        




        
                    