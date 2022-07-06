
import functools
import time

def file_trigger(bucket_name : str ='default', python_ver : str ='python37'):
    def inner_file_trigger(func):
        """Print the runtime of the decorated function"""
        @functools.wraps(func)        
        def wrapper_deploy_file_trigger(*args, **kwargs):                
            # Call the function logic just to test it
            func(*args, **kwargs)
                                                      
            start_time = time.perf_counter()    # 1
            
            func_name = func.__name__
            #print(f"func_name is {func_name}")
                        
            
            template = 'def after_write(data: dict, context): \n \
 \"\"\"Background Cloud Function to be triggered by Cloud Storage. \n \
 This generic function logs relevant data when a file is changed. \n \
                                                            \n  \
 Args: \
 data (dict): The Cloud Functions event payload. \n \
 context (google.cloud.functions.Context): Metadata of triggering event. \n \
 Returns: \n \
 None; the output is written to Stackdriver Logging \n \
 \"\"\"\n'
                                       
            content = template.splitlines()                                            
            func_name, header, body = __parse_function( func )

            content.append("  event_id = data['event_id'] \n  event_type = data['event_type'] \n  bucket_name = data['bucket_name'] \n  file_name = data['file_name'] \n  metageneration = data['metageneration'] \n  timeCreated = data['timeCreated'] \n  updated = data['updated']\n")
            func_call="   (event_id, event_type, bucket_name, file_name, metageneration, timeCreated, updated)".format(func_name)
            content.append(f"  {func_name}(event_id, event_type, bucket_name, file_name, metageneration, timeCreated, updated)")

    
            content = body + content 

            content_str = "\n".join( content )
            print(content_str)

            # Write function to file system
            directory = "/content/deploy_functions"
            import os
            if not os.path.exists(directory):
                os.makedirs(directory)

            f = open("{}/main.py".format(directory), "w")
            f.writelines( content_str )
            f.close()

                        
            cmd = f"gcloud functions deploy {bucket_name.lower()} --trigger-event google.storage.object.finalize --memory 2048MB --entry-point after_write --trigger-resource {bucket_name} --runtime {python_ver} --allow-unauthenticated --source /content/deploy_functions"            
            __run_cmd( cmd )
                                                                                    
            end_time = time.perf_counter()      # 2
            run_time = end_time - start_time    # 3
           
            print(f"Finished {func.__name__!r} in {run_time:.4f} secs")            
        return wrapper_deploy_file_trigger
    return inner_file_trigger    

def bq_udf_function(request_json):
    def inner_bq_udf_function(func):
        """Print the runtime of the decorated function"""
        @functools.wraps(func)        
        def wrapper_inner_bq_udf_function(*args, **kwargs):                
            # Call the function logic just to test it
            func(*args, **kwargs)                    
            start_time = time.perf_counter()    # 1
                        
            import inspect
            
            template = "def after_write(data: dict, context): \
                \"\"\"Background Cloud Function to be triggered by Cloud Storage. \
                This generic function logs relevant data when a file is changed. \
                                                                                \
                Args: \
                    data (dict): The Cloud Functions event payload. \
                    context (google.cloud.functions.Context): Metadata of triggering event. \
                Returns: \
                    None; the output is written to Stackdriver Logging \
                    \
                    data['event_id'] \
                    data['event_type'] \
                    data['bucket_name'] \
                    data['file_name'] \
                    data['metageneration'] \
                    data['timeCreated'] \
                    data['updated'] \
                \"\"\""
                
            lines = inspect.getsource(func)
            lines = lines[1:]
            lines = template +  lines
            
            # Write function to file system
            f = open("main.py", "a")
            f.writelines( lines )
            f.close()
            
            params = f"functions deploy --trigger-event google.storage.object.finalize --memory 2048MB --entry-point after_write --trigger-resource {bucket_name} --runtime {python_ver}"
                                                                                        
            end_time = time.perf_counter()      # 2
            run_time = end_time - start_time    # 3
            print(f"Function {lines}")
            print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
            #return value
        return wrapper_inner_bq_udf_function
    return inner_bq_udf_function   

def __parse_function( func ) -> tuple:
    """ Returns: func_name, func_header, func_body as a list """
    import inspect
    lines : list = inspect.getsource(func).splitlines()           
    lines = lines[1:]
    lines.append('\n')

    header: str = lines[0]

    start_pos = header.find( ' ' )
    end_pos = header.find( '(' )

    func_name = header[start_pos: end_pos]

    return func_name.strip(), header, lines


def __run_cmd( cmd : str ) -> tuple :
    print(cmd)
    cmd_list = cmd.split(' ')

    import subprocess
    import sys

    result = subprocess.run(
        cmd_list,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE
    )

    

    print("stdout:", result.stdout)
    print("stderr:", result.stderr)

    return result.stdout, result.stderr
           

