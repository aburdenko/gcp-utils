import functools
import time



def timer(func):

    import sys
    stdout = sys.stdout
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        sys.stdout = stdout    
        
        print("Finished {func.__name__!r} in {run_time:.4f} secs".format(run_time) )       
        return value

    print('finished')        
    
    return wrapper_timer


def file_trigger(bucket_name='default', python_ver='3.7'):
    def inner_file_trigger(func):
        """Print the runtime of the decorated function"""
        @functools.wraps(func)        
        def wrapper_deploy_file_trigger(*args, **kwargs):                
            # Call the function logic just to test it
            func(*args, **kwargs)

            import subprocess 
            
            # Mutate sequences
            #print(f"Deploying cloud function {func.__name__!r}...")
            # santa_cmd = ['java', '-jar', 'santa.jar', mutation_definition.path]

            # result = subprocess.run(
            # santa_cmd,
            # stderr=subprocess.PIPE,
            # stdout=subprocess.PIPE
            # )
            # print(f"Deployed {func.__name__!r} in {run_time:.4f} secs.")
            # return value
            
        
            #start_time = time.perf_counter()    # 1
            
            

            
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
            # print(f"Function {lines}")
            # print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
            #return value
        return wrapper_deploy_file_trigger
    return inner_file_trigger    


def schedule_trigger(cron_expression):
    # Call the function logic just to test it
    func(*args, **kwargs)    
    print(f"Deploying cloud function {func.__name__!r}...")

    def inner_schedule_trigger(func):

        
        
        """Print the runtime of the decorated function"""
        @functools.wraps(func)
        def wrapper_deploy_schedule_trigger(*args, **kwargs):                
            
                        
            
            
            import subprocess 
            
            # Mutate sequences
            
            #func(*args, **kwargs)    
            # santa_cmd = ['java', '-jar', 'santa.jar', mutation_definition.path]

            # result = subprocess.run(
            # santa_cmd,
            # stderr=subprocess.PIPE,
            # stdout=subprocess.PIPE
            # )
            # print(f"Deployed {func.__name__!r} in {run_time:.4f} secs.")
            # return value
            
        
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
            #print(f"Function {lines}")
            #print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
            #return value
        return wrapper_deploy_schedule_trigger
    return inner_schedule_trigger   

#if __name__ == "__main__":
    




