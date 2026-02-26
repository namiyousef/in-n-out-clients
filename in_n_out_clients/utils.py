from in_n_out_clients.config import GOOGLE_OAUTH_CREDENTIAL_FILE, GOOGLE_OAUTH_TOKEN
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

def get_google_credentials(scopes: list[str]):
    creds = None
    if os.path.exists(GOOGLE_OAUTH_TOKEN):
      creds = Credentials.from_authorized_user_file(GOOGLE_OAUTH_TOKEN, scopes)
      if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        with open(GOOGLE_OAUTH_TOKEN, "w") as f:
          f.write(creds.to_json())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
        GOOGLE_OAUTH_CREDENTIAL_FILE, scopes
      )
      creds = flow.run_local_server(port=0)
      with open(GOOGLE_OAUTH_TOKEN, "w") as f:
        f.write(creds.to_json())
    
    return creds