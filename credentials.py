from oauth2client.client import Credentials, Storage
import google.auth
def get_credentials(service_account_file)->Credentials:
    """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
        Credentials, the obtained credential.
    """
    # SCOPES = ['https://www.googleapis.com/auth/drive']
    # APPLICATION_NAME = "seres kg"

    # from os import path, makedirs
    # home_dir = path.expanduser("~")
    # credential_dir = path.join(home_dir, ".credentials")
    # client_secret_path = path.join(credential_dir,'creds.json')            
    # if not path.exists(credential_dir): 
    #     makedirs(credential_dir)        
        
    # store = Storage(client_secret_path)                
    # creds : Credentials = None

    # # try:
    # #     store.get()
    # # except Exception:
    # #     pass


    # if not creds:

    #     # See https://cloud.google.com/docs/authentication/getting-started
        
    #     creds, _ = google.auth.default() 

    #     if service_account_file:
                
    #         from google.oauth2 import service_account
    #         service_account_file_path = path.join(credential_dir, service_account_file)       
    #         creds = service_account.Credentials.from_service_account_file
    #         (
    #             service_account_file_path
    #             ,["https://www.googleapis.com/auth/cloud-platform"]
    #         )     
                                                                    
    #     if not creds:                                                                                       
    #             from oauth2client import client, tools
    #             flags = tools.argparser.parse_args(args=['--noauth_local_webserver'])                         
    #             flow = client.flow_from_clientsecrets(client_secret_path, SCOPES)
    #             flow.user_agent = APPLICATION_NAME
    #             creds = tools.run_flow(flow, store, flags)

    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file( service_account_file )

    scoped_credentials = creds.with_scopes(
        ['https://www.googleapis.com/auth/cloud-platform'])

    return creds