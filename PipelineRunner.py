import os
import logging
import re


class PipelineRunner():
    """
        Class to instantiate a processing pipeline
    """
    def __init__(self):        
        logging.basicConfig(level=logging.INFO)
        self._logger : Logger = logging.getLogger(__name__)   
        self._first_gcs_path = '' 
                               

    def process(self, module_name, class_name, project_id, gcs_path, first_gcs_path: bool):        
        import re
        class_name = re.sub("(^|[_?!])\s*([a-zA-Z])", lambda p: p.group(0).upper(), class_name)
        class_name = class_name.replace('_','')

        module_name=f"{module_name}.{class_name}"
        
        from datetime import datetime
        import google.cloud.aiplatform as aip
        
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        timestamp_str = str(timestamp)
        from gcputils import Interface
        instance: Interface = self._create_instance(module_name, class_name, project_id, gcs_path, first_gcs_path, timestamp_str)                                                 
                    
        # run this
        res =  instance.process()                                                                                     



    def _create_instance(self, module_name : str , class_name : str , project_id : str , input_gcs_uri : str , first_gcs_path : str , timestamp_str : str ):                                       
        import importlib                        
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)                
        
        instance = class_(project_id, input_gcs_uri, first_gcs_path, timestamp_str)         
            
        return instance
    

    





                
                
        

        

