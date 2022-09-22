from gcputils.FileConverter import FileConverter
from gcputils.hcls_nlp import analyze_entities
from gcputils.cloud_storage import upload_str_to_bucket
from google.cloud import bigquery

class BqImportFileConverter(FileConverter):

    from google.cloud import bigquery
    from functools import lru_cache

    #@lru_cache(maxsize=None)
    def _bq_query( self, query : str, key : str = None ):   
        """
            Loading data to BigQuery 
        """    
        key = 'input_gcs_uri'
        from google.cloud import bigquery
        
        #print(project)
        client = bigquery.Client(credentials=self._creds, project=self._project_id)  

        query_job = client.query(query)

        results = query_job.result() # Wait for the job to complete.

        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        # Start the query, passing in the extra configuration.
        query_job = client.query(query, job_config=job_config)  # Make an API request.
        
        rows = query_job.result()
        print('Job finished.') 

        keys = [row.get(key, None) for row in rows]
        # print("The query data:")
        # for row in rows:
        #     print(f"value for key {key} is: {row.get(key, None)}")

        return list(keys)

    def _already_imported( self, gcs_input_path : str, key: str = 'input_gcs_uri')->bool:
        
        table = 'Entity' if 'entity' in gcs_input_path else 'Document'
        
        sql = f"select {key} from entities.{table}"
        
        print( self._creds )
        print( self._project_id )
        try:
            gcs_uris: list = self._bq_query(sql, key=key)        
        except:
            return False

        print(f"Checking if {gcs_input_path} is already loaded...")                        
        print(f"bool: {gcs_input_path in gcs_uris}")

        return (gcs_input_path in gcs_uris)


    
    def _bq_import(self, dataset_id, table_name, input_gcs_uri, src_fmt: bigquery.SourceFormat = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition : bigquery.WriteDisposition = bigquery.WriteDisposition().WRITE_TRUNCATE):   
        """
            Loading data to BigQuery 
        """            
        #print(project)
        
        bq = bigquery.Client( credentials = self._creds, project = self._project_id )  
        bq.create_dataset( dataset_id, exists_ok=True)
        
        dataset_ref = bq.dataset(dataset_id)  
        table_id = f"{dataset_id}.{table_name}"
        table_ref = dataset_ref.table(table_name)

        # determine uploading options
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = write_disposition
        job_config.source_format = src_fmt
        job_config.autodetect = True       

        load_job = bq.load_table_from_uri(
            input_gcs_uri,
            table_ref,
            job_config = job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.        
        print('Job finished.') 
       
    def process( self, **kwargs ):       
        print(f"clean_path : {self._clean_file_path}")
        print(f"file_prefix : {self._file_prefix}")
        print(f"sub_folder : {self._sub_folder}")
        print(f"bucket_name : {self._bucket_name}")
        print(f"file_name : {self._file_name}")


        str_item=self._content
        creds=self._creds
        project=self._project_id

        if not self._already_imported( self._input_gcs_uri):
            table_name : str = self._file_name.split('_')[0]
            table_name = table_name.capitalize()        
            
            write_disposition : bigquery.WriteDisposition = bigquery.WriteDisposition().WRITE_APPEND
            try:
                self._bq_import( 'entities', table_name,  self._input_gcs_uri, src_fmt=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=write_disposition ) 
            except:
                write_disposition : bigquery.WriteDisposition = bigquery.WriteDisposition().WRITE_TRUNCATE
                self._bq_import( 'entities', table_name,  self._input_gcs_uri, src_fmt=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition=write_disposition ) 




        
                    