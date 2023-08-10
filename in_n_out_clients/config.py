import os

GOOGLE_OAUTH_CREDENTIAL_FILE = os.environ.get(
    "GOOGLE_OAUTH_CREDENTIAL_FILE", "google_oauth_credentials.json"
)
GOOGLE_OAUTH_TOKEN = os.environ.get(
    "GOOGLE_OAUTH_TOKEN", "google_oauth_token.json"
)
