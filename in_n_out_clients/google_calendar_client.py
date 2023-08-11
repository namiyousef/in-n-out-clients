from __future__ import print_function

import datetime
import logging
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from in_n_out_clients.config import (
    GOOGLE_OAUTH_CREDENTIAL_FILE,
    GOOGLE_OAUTH_TOKEN,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    GOOGLE_OAUTH_CREDENTIAL_FILE, SCOPES
                )
            except FileNotFoundError as file_not_found_error:
                raise ConnectionError(
                    f"Could not find Google OAuth Credentals file: `{GOOGLE_OAUTH_CREDENTIAL_FILE}`"
                ) from file_not_found_error

            logger.info("Running flow...")
            credentials = flow.run_local_server()
            with open(GOOGLE_OAUTH_TOKEN, "w") as f:
                f.write(credentials.to_json())

        logger.info("Initialising client...")
        client = build("calendar", "v3", credentials=credentials)

        return client

    # ignore, replace, fail
    # -- ignore: if you find conflicts, you ignore the request.
    # No error returned but should indicate that table was not created
    # -- append: if you find conflcits, you add the data nonetheless

    # -- fail: if you find conflicts, you fail
    # -- replace: if you find conflicts, you replacde

    def create_calendar(
        calendar,
        on_asset_conflict="ignore",
    ):
        None

    def create_events(
        self,
        events,
        calendar="primary",
        on_asset_conflict="ignore",
        on_data_conflict="ignore",
        data_conflict_properties: list | None = None,
        create_calendar_if_not_exist=False,  # how to specify HOW to create the calendar...!
    ):
        logger.info("Getting list of calendars available...")
        try:
            calendars = self.client.calendarList().list().execute()["items"]
            logger.debug(f"Got {len(calendars)} calendars")
        except HttpError as http_error:
            return {
                "status_code": http_error.status_code,
                "msg": f"Could not read calendar information. Reason: {http_error}",
            }

        _calendars_available = {calendar["summary"] for calendar in calendars}
        if calendar not in _calendars_available:
            logger.info(f"calendar=`{calendar}` does not exist")
            if create_calendar_if_not_exist:
                logger.info(f"Creating new calendar=`{calendar}`...")
                raise NotImplementedError(
                    (
                        f"Could not find calendar={calendar}. At the moment "
                        "there is no support for creating new calendars from "
                        "the client. Please do this manually on the web and "
                        "try again."
                    )
                )
            else:
                return {
                    "status_code": 404,
                    "msg": f"Could not find calendar={calendar}. If you wish to "
                    "create it, set table creation to `True`",
                }

        if on_asset_conflict == "fail":
            return {
                "status_code": 409,
                "msg": f"calendar={calendar} exists and on_asset_conflict=`{on_asset_conflict}`. If you wish to edit calendar please change conflict_resolution_strategy",
            }

        if on_asset_conflict == "ignore":
            return {
                "status_code": 204,
                "msg": (
                    f"calendar={calendar} exists but request dropped since "
                    "on_asset_conflict=`ignore`"
                ),
            }

        if on_asset_conflict == "replace":
            # need to delete the calendar, then create a new calendar!
            # TOOD not sure If I want to allow this tbh!
            raise NotImplementedError(
                "There is currently no support for replace strategy."
            )

        # TODO need to come up with proper strat for this
        if on_data_conflict != "append":
            raise NotImplementedError(
                "There is currently only support for data level append"
            )

        # if ignore --> if there is a conflict, then don't commit the conflicting item
        # if append --> don't do any checks
        # if replace --> if there is a conflict, then delete it and write the new one
        # if fail --> if there is any conflcit, then fail whole thing. Conflicts need to be checked before weriting
        # on fail, needs to cleanup if a new calendar HAD been created... this is complex!
        if on_data_conflict != "append":
            # check that the input events contain the on conflict columns
            # if not, AND if on_conflict is fail, then you MUST delete the table created if it had been created
            # ?
            None
        events_session = self.client.events()
        failed_writes = []
        # if failed writes, need to return 207 code. E.g. no guarantee of success
        # if all failed writes, need to return failure, e.g. 400
        for event_id, event in enumerate(events):
            try:
                events_session.insert(
                    calendarId=calendar, body=event
                ).execute()
            except HttpError as http_error:
                status_code = http_error.status_code
                logger.error(
                    (
                        f"Failed to create event {event_id+1}/{len(events)}. "
                        f"Reason: {http_error}"
                    )
                )
                failed_writes.append(
                    {
                        "msg": http_error,
                        "data": event,
                        "status_code": status_code,
                    }
                )
        if len(failed_writes) == len(events):
            return {
                "msg": "Failed to update calendar",
                "status_code": 400,
                "data": failed_writes,
            }
        elif not failed_writes:
            return {
                "msg": "Successfully wrote data to calendar",
                "status_code": 201,
            }
        else:
            return {
                "msg": "At least some events failed to create",
                "status_code": 207,
                "data": failed_writes,
            }

    # should always return a json wherever possible! And add a status code too!


# If modifying these scopes, delete the file token.json.
'''SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


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
        print("An error occurred: %s" % error)'''


if __name__ == "__main__":
    client = GoogleCalendarClient()

    client.create_events("primary", "primary")
