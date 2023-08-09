from __future__ import print_function

import datetime
import os.path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


from in_n_out_clients.config import (
    GOOGLE_OAUTH_CREDENTIAL_FILE,
    GOOGLE_OAUTH_TOKEN,
)

SCOPES = ["https://www.googleapis.com/auth/calendar"]


# TODO need to figure out authentication...
class GoogleCalendarClient:
    def __init__(
        self,
    ):
        self.client = self.initialise()

    def initialise(
        self,
    ):
        credentials = None
        if os.path.exists(GOOGLE_OAUTH_TOKEN):
            logger.info("Detected google oauth token... validating...")
            credentials = Credentials.from_authorized_user_file(
                GOOGLE_OAUTH_TOKEN, SCOPES
            )
            if credentials.expired and credentials.refresh_token:
                logger.info("Credentials have expired... refreshing...")
                credentials.refresh(Request())
                with open(GOOGLE_OAUTH_TOKEN, "w") as f:
                    f.write(credentials.to_json())
        else:
            # TODO initial initialisation not working... need to fix this
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_OAUTH_CREDENTIAL_FILE, SCOPES
            )
            print(flow.__dict__)

        logger.info("Initialising client...")
        client = build("calendar", "v3", credentials=credentials)

        return client

    # ignore, replace, fail
    # -- ignore: if you find conflicts, you ignore the request
    # -- append: if you find conflcits, you add the data nonetheless
    # -- fail: if you find conflicts, you fail
    # -- replace: if you find conflicts, you replacde
    def create_events(
        self,
        events,
        calendar="primary",
        conflict_resolution_strategy="ignore",
        create_calendar=False,
    ):
        if calendar != "primary":
            try:
                calendars = (
                    self.client.calendarList().list().execute()["items"]
                )
            except HttpError as http_error:
                raise Exception(
                    f"Could not read calendar information. Reason: {http_error}"
                )
            _calendars_available = {
                calendar["summary"] for calendar in calendars
            }
            if calendar not in _calendars_available and create_calendar:
                raise NotImplementedError(
                    f"Could not find calendar={calendar}. At the moment there is no support for creating new calendars from the client. Please do this manually on the web and try again."
                )

        if conflict_resolution_strategy != "append":
            raise NotImplementedError(
                "Currently only supports append conflict resolution strategy"
            )

        events_session = self.client.events()
        for event_id, event in enumerate(events):
            try:
                events_session.insert(
                    calendarId=calendar, body=event
                ).execute()
            except HttpError as http_error:
                logger.error(
                    f"Failed to create event {event_id+1}/{len(events)}. Reason: {http_error}"
                )


# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


def main():
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "creds.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("calendar", "v3", credentials=creds)

        print(service.calendarList().list().execute()["items"])

        # Call the Calendar API
        now = (
            datetime.datetime.utcnow().isoformat() + "Z"
        )  # 'Z' indicates UTC time
        print("Getting the upcoming 10 events")
        events_result = (
            service.events()
            .list(
                calendarId="primary",
                timeMin=now,
                maxResults=10,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )
        events = events_result.get("items", [])

        if not events:
            print("No upcoming events found.")
            return

        # Prints the start and name of the next 10 events
        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))
            print(start, event["summary"])

    except HttpError as error:
        print("An error occurred: %s" % error)


if __name__ == "__main__":
    client = GoogleCalendarClient()

    client.create_events("primary", "primary")
