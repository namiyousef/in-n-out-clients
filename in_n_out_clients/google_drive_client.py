from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


class GoogleDriveClient:
    def __init__(self):
        pass

    def initialise_client(self):
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        self.drive = GoogleDrive(gauth)

    def query(self):
        pass
