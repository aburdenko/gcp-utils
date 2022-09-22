import os
import logging
from logging import Logger
import re

from google.cloud import documentai as documentai


class PipelineRunner():
    """
        Class to instantiate a processing pipeline
    """
    def __init__(self):        
        logging.basicConfig(level=logging.INFO)
        self._logger : Logger = logging.getLogger(__name__)   
        self._first_gcs_path = '' 
                               

    def process(
            self
            , module_name : str
            , class_name : str
            , project_id : str
            , gcs_path : str
            , first_gcs_path : str
            , raw_text_file_path : str
            , hcls_nl_json_uri : str            
            , location : str = None
            , proc_id : str = None
        ):        
        import re
        class_name = re.sub("(^|[_?!])\s*([a-zA-Z])", lambda p: p.group(0).upper(), class_name)
        class_name = class_name.replace('_','')

        module_name=f"{module_name}.{class_name}"
        
        from datetime import datetime
        import google.cloud.aiplatform as aip
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        timestamp_str = str(timestamp)
        from gcputils import Interface
        # instance: Interface = self._create_instance(
        #     module_name
        #     , class_name
        #     , project_id
        #     , gcs_path
        #     , first_gcs_path
        #     , raw_text_file_path 
        #     , hcls_nl_json_uri
        #     , timestamp_str)      
        
                                                                                            
        if module_name == 'gcputils.PdfFileConverter':            
            from gcputils.PdfFileConverter import PdfFileConverter            
            instance : PdfFileConverter = self._create_instance(
                module_name
                , class_name
                , project_id
                , gcs_path
                , first_gcs_path
                , raw_text_file_path 
                , hcls_nl_json_uri
                , timestamp_str )      

            instance.location = location
            instance.proc_id = proc_id
        else:
            instance: Interface = self._create_instance(
                module_name
                , class_name
                , project_id
                , gcs_path
                , first_gcs_path
                , raw_text_file_path 
                , hcls_nl_json_uri
                , timestamp_str )     

        # run this
        res =  instance.process()                      



    def _create_instance(self
        , module_name : str 
        , class_name : str 
        , project_id : str 
        , input_gcs_uri : str 
        , first_gcs_path : str 
        , raw_text_file_path : str
        , hcls_nl_json_uri : str
        , timestamp_str : str ):                                       
        import importlib                        
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)                
        
        instance = class_(project_id, input_gcs_uri, first_gcs_path, raw_text_file_path, hcls_nl_json_uri, timestamp_str)         
            
        return instance
    

    





                
                
        

        

