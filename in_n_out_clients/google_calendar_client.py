from __future__ import print_function

import logging
import os.path
from typing import List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from in_n_out_clients.config import (
    GOOGLE_OAUTH_CREDENTIAL_FILE,
    GOOGLE_OAUTH_TOKEN,
)
from in_n_out_clients.in_n_out_types import (
    APIResponse,
    ConflictResolutionStrategy,
)

logging.basicConfig(level=logging.DEBUG)
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
            raise Exception() from key_error  # TODO revisit this
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
                        print(updated_calendar)

                    if on_data_conflict == "ignore":
                        pass

                    if on_data_conflict == "fail":
                        pass

        created_calendar = self.client.calendars().insert(body=body).execute()
        print(created_calendar)

    def _get_calendars(self):
        logger.info("Getting list of calendar available...")
        calendars = self.client.calendarList().list().execute()["items"]
        logger.debug(f"Got {len(calendars)} calendars")
        return calendars

    def _generate_events_conflict_metadata(self, event_conflict_identifiers):
        CONFLICT_PROPERTIES_MAP = {
            "summary": lambda x: ("q", x),
            "iCalUID": lambda x: ("iCalUID", x),
        }

        conflict_metadata = {}
        for (
            conflict_property,
            conflict_value,
        ) in event_conflict_identifiers.items():
            conflict_query_generator = CONFLICT_PROPERTIES_MAP.get(
                conflict_property
            )
            if conflict_query_generator is None:
                raise NotImplementedError(
                    f"Tried to find conflicts using calendar metadata `{conflict_property}` but there is currently no support for this"
                )

            key, value = conflict_query_generator(conflict_value)
            if key in conflict_metadata:
                raise Exception("not expected")

            conflict_metadata[key] = value

        return conflict_metadata

    # TODO problem with doing it this way is that we won't be able to expose which parameters are doing what!

    def _write(
        self,
        table_name: str,
        data,
        on_data_conflict: str = "fail",
        on_asset_conflict: str = "append",
        data_conflict_properties: List[str] | None = None,
    ):
        """_summary_

        :param table_name: _description_
        :type table_name: _type_
        :param data: _description_
        :type data: _type_
        :param on_asset_conflict: _description_
        :type on_asset_conflict: _type_
        :param on_data_conflict: _description_
        :type on_data_conflict: _type_
        :param data_conflict_properties: _description_
        :type data_conflict_properties: _type_
        :raises NotImplementedError: _description_
        :raises NotImplementedError: _description_
        :raises NotImplementedError: _description_
        :return: _description_
        :rtype: _type_
        """

        # TODO this mapping should be made into a global variable and exposed as an endpoint so user can know what each thing means in context of each client
        resp = self.create_events(
            calendar_id=table_name,
            events=data,
            on_asset_conflict=on_asset_conflict,
            on_data_conflict=on_data_conflict,
            data_conflict_properties=data_conflict_properties,
        )

        return resp

    # calendar_id is what? not the database, because database should be fixed per connection
    # could be the dataset, OR table_name. But in this case, table_name makes more sense
    # an event is the data you are writing to a specific table!
    def create_events(
        self,
        calendar_id: str,
        events: list,
        on_asset_conflict: str = "append",
        on_data_conflict: str = "fail",
        data_conflict_properties: list | None = None,
        create_calendar_if_not_exist: bool = False,  # how to specify HOW to create the calendar...!
    ) -> APIResponse:
        """Function to add events to a calendar.

        :param calendar_id: id of the calendar. See calendar resource
        for more information:
        https://developers.google.com/calendar/api/v3/reference/calendars
         :param events: events to create. See for more information:
        https://developers.google.com/calendar/api/v3/reference/events/insert
        :param calendar_id: id of the calendar. See calendar resource for more information
        : https: //developers.google.com/calendar/api/v3/reference/calendars
        :param events: events to create. See for more information
        : https: //developers.google.com/calendar/api/v3/reference/events/insert
        :param on_asset_conflict: specify behaviour if calendar_id
                already exists, defaults to "ignore"
        :param on_data_conflict: specify behaviour if event already exists, defaults to "ignore"
        :param data_conflict_properties: event properties to check for conflicts, defaults to None
        :param create_calendar_if_not_exist: flag to create a calendar
                if it does not already exist, defaults to False
        """
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
        else:
            logger.info(
                f"Calendar with calendar_id=`{calendar_id}` exists... checking for conflicts..."
            )
            match on_asset_conflict:
                case ConflictResolutionStrategy.FAIL:
                    _msg = f"calendar with calendar_id=`{calendar_id}` exists and on_asset_conflict=`{on_asset_conflict}`. If you wish to edit calendar please change conflict_resolution_strategy"
                    logger.error(_msg)
                    return {
                        "status_code": 409,
                        "msg": _msg,
                    }
                case ConflictResolutionStrategy.IGNORE:
                    _msg = (
                        f"calendar_id=`{calendar_id}` exists but request dropped since "
                        "on_asset_conflict=`ignore`"
                    )
                    logger.info(_msg)
                    return {"status_code": 200, "msg": _msg}
                case ConflictResolutionStrategy.REPLACE:
                    _msg = f"calendar with calendar_id=`{calendar_id}` exists and on_asset_conflict=`{on_asset_conflict}`. There is currently no support for this."
                    # need to delete the calendar, then create a new calendar!
                    # TOOD not sure If I want to allow this tbh!
                    logger.error(_msg)
                    raise NotImplementedError(_msg)
                case _:
                    logger.info(
                        "on_asset_conflict set to `append`... ignoring any conflicts..."
                    )

        if on_data_conflict == ConflictResolutionStrategy.REPLACE:
            raise NotImplementedError("No support for this yet")

        # if ignore --> if there is a conflict, then don't commit the conflicting item
        # if append --> don't do any checks
        # if replace --> if there is a conflict, then delete it and write the new one
        # if fail --> if there is any conflcit, then fail whole thing. Conflicts need to be checked before weriting
        # on fail, needs to cleanup if a new calendar HAD been created... this is complex!

        events_session = self.client.events()

        events_to_create = dict(enumerate(events))
        num_events_to_create = len(events_to_create)
        logger.info(f"Got {num_events_to_create} events to write")

        if on_data_conflict != ConflictResolutionStrategy.APPEND:
            logger.info(f"Checking {num_events_to_create} for conflicts...")
            conflict_events = []
            for event_count, event_id in enumerate(
                list(events_to_create.keys())
            ):
                logger.debug(
                    f"Checking event {event_count+1}/{num_events_to_create}..."
                )
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
                logger.debug(
                    f"Searching calendar_id=`{calendar_id}` for events with the following properties: {_data_conflict_properties}"
                )
                try:
                    event_list = events_session.list(
                        calendarId=calendar_id, **conflict_metadata
                    ).execute()
                except HttpError as http_error:
                    raise Exception(
                        f"There was a failure in looking for conflicts for event_id=`{event_id}`. Reason: {http_error}"
                    ) from http_error

                conflicting_events = event_list["items"]
                num_conflicting_events = len(conflicting_events)

                if conflicting_events:
                    logger.debug(
                        f"Event {event_count+1}/{num_events_to_create} conflicts with {num_conflicting_events}..."
                    )
                    conflicting_event_ids = [
                        _conflict_event["id"]
                        for _conflict_event in conflicting_events
                    ]

                    match on_data_conflict:
                        case ConflictResolutionStrategy.FAIL:
                            logger.error(
                                (
                                    "Exiting process since "
                                    "on_data_conflict=`fail`..."
                                )
                            )
                            return {
                                "status_code": 409,
                                "msg": (
                                    f"At least one event to write conflicts with events from calendar=`{calendar_id}` on the following conflict properties `{data_conflict_properties}`"
                                ),
                                "data": [
                                    {
                                        "event_to_write": event,
                                        "event_id": event_id,
                                        "id_of_events_that_conflict": conflicting_event_ids,
                                    }
                                ],
                            }
                        case ConflictResolutionStrategy.IGNORE:
                            logger.info(
                                f"Dropping event_id `{event_id}` from request since on_data_conflict=`ignore`..."
                            )
                            conflict_events.append(
                                {
                                    "event_to_write": events_to_create.pop(
                                        event_id
                                    ),
                                    "event_id": event_id,
                                    "id_of_events_that_conflict": conflicting_event_ids,
                                }
                            )
                else:
                    logger.info(
                        f"Did not find any conflicts for event_id=`{event_id}`"
                    )

                # if there is a conflict, then
            # check that the input events contain the on conflict columns
            # if not, AND if on_conflict is fail, then you MUST delete the table created if it had been created
            # ?
        # if failed writes, need to return 207 code. E.g. no guarantee of success
        # if all failed writes, need to return failure, e.g. 400
        if not events_to_create:
            _msg = "No events to create"
            logger.info(_msg)
            return_msg = {"msg": _msg, "status_code": 200}
            if (
                on_data_conflict == ConflictResolutionStrategy.IGNORE
                and conflict_events
            ):
                return_msg["data"] = [
                    {"ignored_events_due_to_conflict": conflict_events}
                ]

            return return_msg

        num_events_to_create = len(events_to_create)
        logger.info(f"Writing {num_events_to_create} events...")
        failed_writes = []
        for event_count, (event_id, event) in enumerate(
            events_to_create.items()
        ):
            # TODO add debug logs
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
                failed_writes.append(
                    {
                        "msg": http_error,
                        "data": {"event": event, "event_id": event_id},
                        "status_code": status_code,
                    }
                )

        num_failed_writes = len(failed_writes)
        if not failed_writes:
            _msg = f"Successfully wrote {num_events_to_create} events to calendar. No failures occurred in write process."
            logger.info(_msg)
            return_msg = {
                "msg": _msg,
                "status_code": 201,
            }

            if (
                on_data_conflict == ConflictResolutionStrategy.IGNORE
                and conflict_events
            ):
                return_msg["data"] = [
                    {"ignored_events_due_to_conflict": conflict_events}
                ]

        else:
            logger.info(f"{num_failed_writes} events failed to create")
            return_msg = {
                "data": [{"reason_for_failure": failed_writes}],
            }

            if num_failed_writes == num_events_to_create:
                _msg = "None of the events were successfully created due to write errors"
                logger.error(_msg)
                return_msg.update(
                    {
                        "msg": _msg,
                        "status_code": 400,
                    }
                )
            else:
                _msg = f"{num_failed_writes}/{num_events_to_create} events failed to create, but others were successful"
                logger.info(_msg)
                return_msg.update(
                    {
                        "msg": _msg,
                        "status_code": 207,
                    }
                )

            if (
                on_data_conflict == ConflictResolutionStrategy.IGNORE
                and conflict_events
            ):
                return_msg["data"].append(
                    {"ignored_events_due_to_conflict": conflict_events}
                )

        return return_msg


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
            on_data_conflict="replace",
        )
    )
