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

    def _get_calendar_unique_identifier(
        self, calendar_body, data_conflict_properties
    ):
        try:
            unique_keys = {
                conflict_key: calendar_body[conflict_key]
                for conflict_key in data_conflict_properties
            }
        except KeyError as key_error:
            raise Exception()
        else:
            unique_identifier = tuple(unique_keys.values())
            return unique_identifier

    def check_calendar_exists(self):
        pass

    # when you say replace, does that meal replace the conflicting metadata... or replace
    # the entire thing??
    def create_calendar(
        # TODO this is good code but needs a thorough review. Plan to expose this standaloen
        self,
        calendar_name,
        calendar_properties: dict | None = None,
        on_data_conflict="fail",
        data_conflict_properties: list | None = None,
    ):
        body = {"summary": calendar_name}
        if calendar_properties is not None:
            body.update(calendar_properties)

        if on_data_conflict != "append":
            if data_conflict_properties is None:
                data_conflict_properties = list(body.keys())

                calendars = self._get_calendars()

                # collect calendars by properties
                new_calendar_unique_identifier = (
                    self._get_calendar_unique_identifier(
                        body, data_conflict_properties
                    )
                )
                unique_calendars = set()
                for calendar in calendars:
                    calendar_unique_identifier = (
                        self._get_calendar_unique_identifier(
                            calendar, data_conflict_properties
                        )
                    )
                    unique_calendars.add(calendar_unique_identifier)

                if (
                    new_calendar_unique_identifier
                    in calendar_unique_identifier
                ):
                    if on_data_conflict == "replace":
                        updated_calendar = (
                            self.client.calendars()
                            .update(calendarId=calendar["id"], body=body)
                            .execute()
                        )

                    if on_data_conflict == "ignore":
                        pass

                    if on_data_conflict == "fail":
                        pass

        created_calendar = self.client.calendars().insert(body=body).execute()

    def _get_calendars(self):
        logger.info("Getting list of calendar available...")
        calendars = self.client.calendarList().list().execute()["items"]
        logger.debug(f"Got {len(calendars)} calendars")
        return calendars

    def _generate_events_conflict_metadata(self, event_conflict_identifiers):
        CONFLICT_PROPERTIES_MAP = {"summary": lambda x: ("q", x)}

        conflict_metadata = {}
        for (
            conflict_property,
            conflict_value,
        ) in event_conflict_identifiers.items():
            conflict_query_generator = CONFLICT_PROPERTIES_MAP.get(
                conflict_property
            )
            if conflict_query_generator is None:
                raise NotImplementedError()

            key, value = conflict_query_generator(conflict_value)
            if key in conflict_metadata:
                raise Exception("not expected")

            conflict_metadata[key] = value

        return conflict_metadata

    def create_events(
        self,
        calendar_id,
        events,
        on_asset_conflict="ignore",
        on_data_conflict="ignore",
        data_conflict_properties: list | None = None,
        create_calendar_if_not_exist=False,  # how to specify HOW to create the calendar...!
    ):
        try:
            calendars = self._get_calendars()
        except HttpError as http_error:
            return {
                "status_code": http_error.status_code,
                "msg": f"Could not read calendar information. Reason: {http_error}",
            }

        _calendars_available = {calendar["id"] for calendar in calendars}
        if calendar_id not in _calendars_available:
            logger.info(
                f"calendar with calendar_id=`{calendar_id}` does not exist"
            )
            # TODO if you do decide to create, then failures with on_data_conflict may require you
            # to delete it back! Keep this in mind!
            if create_calendar_if_not_exist:
                logger.info(f"Creating new calendar=`{calendar_id}`...")
                raise NotImplementedError(
                    (
                        f"Could not find calendar with calendar_id=`{calendar_id}`. At the moment "
                        "there is no support for creating new calendars from "
                        "the client. Please do this manually on the web and "
                        "try again."
                    )
                )
            else:
                return {
                    "status_code": 404,
                    "msg": f"Could not find calendar with calendar_id=`{calendar_id}`. If you wish to "
                    "create it, set table creation to `True`",
                }

        if on_asset_conflict == "fail":
            return {
                "status_code": 409,
                "msg": f"calendar with calendar_id=`{calendar_id}` exists and on_asset_conflict=`{on_asset_conflict}`. If you wish to edit calendar please change conflict_resolution_strategy",
            }

        if on_asset_conflict == "ignore":
            return {
                "status_code": 204,
                "msg": (
                    f"calendar_id=`{calendar_id}` exists but request dropped since "
                    "on_asset_conflict=`ignore`"
                ),
            }

        if on_asset_conflict == "replace":
            # need to delete the calendar, then create a new calendar!
            # TOOD not sure If I want to allow this tbh!
            raise NotImplementedError(
                "There is currently no support for replace strategy."
            )

        if on_data_conflict == "replace":
            raise NotImplementedError("No support for this yet")

        # if ignore --> if there is a conflict, then don't commit the conflicting item
        # if append --> don't do any checks
        # if replace --> if there is a conflict, then delete it and write the new one
        # if fail --> if there is any conflcit, then fail whole thing. Conflicts need to be checked before weriting
        # on fail, needs to cleanup if a new calendar HAD been created... this is complex!

        events_session = self.client.events()

        events_to_create = {
            event_id: event for event_id, event in enumerate(events)
        }

        if on_data_conflict != "append":
            logger.info("Checking events for conflicts...")
            conflict_events = {}
            for event_count, event_id in enumerate(
                list(events_to_create.keys())
            ):
                event = events_to_create[event_id]
                if data_conflict_properties is None:
                    _data_conflict_properties = list(event.keys())
                else:
                    _data_conflict_properties = data_conflict_properties

                event_conflict_identifiers = {
                    conflict_property: event[conflict_property]
                    for conflict_property in _data_conflict_properties
                }
                conflict_metadata = self._generate_events_conflict_metadata(
                    event_conflict_identifiers
                )
                event_list = events_session.list(
                    calendarId=calendar_id, **conflict_metadata
                ).execute()

                conflicting_events = event_list["items"]

                if conflicting_events:
                    conflicting_event_ids = [
                        _conflict_event["id"]
                        for _conflict_event in conflicting_events
                    ]
                    if on_data_conflict == "fail":
                        return {
                            "status_code": 409,
                            "msg": f"At least one event exists with the following conflict properties `{data_conflict_properties}`",
                            "data": {
                                "event_to_write": event,
                                "id_of_events_that_conflict": conflicting_event_ids,
                            },
                        }  # return appropriate response code! this is an early exit

                    if on_data_conflict == "ignore":
                        conflict_events[event_id] = {
                            "event_to_write": events_to_create.pop(event_id),
                            "id_of_events_that_conflict": conflicting_event_ids,
                        }
                    # there is a conflict!

                # if there is a conflict, then
            # check that the input events contain the on conflict columns
            # if not, AND if on_conflict is fail, then you MUST delete the table created if it had been created
            # ?
        # if failed writes, need to return 207 code. E.g. no guarantee of success
        # if all failed writes, need to return failure, e.g. 400
        if not events_to_create:
            return_msg = {"msg": "No events to create", "status_code": 200}
            if on_data_conflict == "ignore" and conflict_events:
                return_msg["ignored_events_due_to_conflict"] = conflict_events

            return return_msg

        num_events_to_create = len(events_to_create)
        logger.info(f"Writing {num_events_to_create} events...")
        failed_events = {}
        for event_count, (event_id, event) in enumerate(
            events_to_create.items()
        ):
            try:
                # TODO remember, replace needs to go here (e.g. update!)
                events_session.insert(
                    calendarId=calendar_id, body=event
                ).execute()
            except HttpError as http_error:
                status_code = http_error.status_code
                logger.error(
                    (
                        f"Failed to create event {event_count+1}/{num_events_to_create}. "
                        f"Reason: {http_error}"
                    )
                )
                failed_events[event_id] = {
                    "msg": http_error,
                    "data": event,
                    "status_code": status_code,
                }
        num_failed_writes = len(failed_events)
        if not failed_events:
            return_msg = {
                "msg": "Successfully wrote events to calendar",
                "status_code": 201,
            }

            if on_data_conflict == "ignore" and conflict_events:
                return_msg["data"] = {}
        else:
            return_msg = {
                "data": {"reasons": failed_events},
            }

            if num_failed_writes == num_events_to_create:
                return_msg[
                    "msg"
                ] = "None of the events were successfully created due to write errors"
            else:
                return_msg["msg"] = "At least some events failed to create"

        if on_data_conflict == "ignore" and conflict_events:
            return_msg["data"][
                "ignored_events_due_to_conflict"
            ] = conflict_events

        return return_msg

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

    print(
        client.create_events(
            "yousefofthenamis@gmail.com",
            [
                {
                    "summary": "Testing",
                    "start": {
                        "dateTime": "2023-08-12T17:00:00",
                        "timeZone": "UTC",
                    },
                    "end": {
                        "dateTime": "2023-08-12T17:15:00",
                        "timeZone": "UTC",
                    },
                },
                {
                    "summary": "Testing3",
                    "start": {
                        "dateTime": "2023-08-12T17:00:00",
                        "timeZone": "UTC",
                    },
                    "end": {
                        "dateTime": "2023-08-12T17:15:00",
                        "timeZone": "UTC",
                    },
                },
            ],
            data_conflict_properties=["summary"],
            on_asset_conflict="append",
            on_data_conflict="ignore",
        )
    )
