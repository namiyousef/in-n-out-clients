import base64
import logging
import os
from email.mime.text import MIMEText
from typing import List, Optional, TypedDict, Union

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from in_n_out_clients.utils import get_google_credentials

logger = logging.getLogger(__name__)


class Messages(TypedDict):
    messages: list[dict]
    next_page_token: Optional[str]


SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
]


class GmailClient:
    def __init__(self, scopes: Optional[list[str]] = None):
        self.client = self._initialise(scopes or SCOPES)

    def _initialise(self, scopes: list[str]):
        creds = get_google_credentials(scopes)
        client = build("gmail", "v1", credentials=creds)
        return client

    @staticmethod
    def _get_messages(
        message_resource: Resource,
        query: str,
        max_results: int,
        next_page_token: None,
    ):
        logger.debug(
            "Fetching messages with query=%s, max_results=%d, next_page_token=%s",
            query,
            max_results,
            next_page_token,
        )
        try:
            return message_resource.list(
                userId="me",
                q=query,
                maxResults=max_results,
                pageToken=next_page_token,
            ).execute()
        except HttpError as http_error:
            logger.error(f"Failed to get messages: {http_error}")
            raise Exception from http_error

    def list_messages(
        self,
        query: str = "is:unread",
        max_results: int = 1,
        next_page_token: None = None,
        get_all: bool = True,
    ) -> List[dict]:
        """Return a list of threads matching `query` (defaults to unread).

        Each returned dict contains: `threadId`, `messageCount`, `snippet`,
        `subject` (from the most recent message) and `from` (most recent sender).

        :param query: Query to filter the search results
        :param max_results: maximum results to fetch in a page.
        """

        message_resource = self.client.users().messages()

        messages = GmailClient._get_messages(
            message_resource, query, max_results, next_page_token
        )
        if get_all:
            while messages.get("nextPageToken"):
                next_page_token = messages["nextPageToken"]
                next_page = GmailClient._get_messages(
                    message_resource, query, max_results, next_page_token
                )
                messages["messages"].extend(next_page.get("messages", []))
                messages["nextPageToken"] = next_page.get("nextPageToken")

        messages.pop("resultSizeEstimate")

        return messages
    
    # TODO copilot generated. Review and cleanup
    def get_attachment(
        self,
        message_id: str,
        attachment_id: str,
        content_type: str,
        filename: str,
        output_dir: str = ".",
    ) -> str:
        """Download an attachment and save it to disk based on content type.

        Organizes files into subdirectories by category (images/, documents/, etc.)

        :param message_id: Gmail message ID
        :param attachment_id: Gmail attachment ID
        :param content_type: MIME content type (e.g., "application/pdf", "image/png")
        :param filename: Original filename for the attachment
        :param output_dir: Base directory to save files (default: current directory)
        :return: Path to the saved file
        """

        # Fetch and decode attachment
        resp = self.client.users().messages().attachments().get(
            userId="me", id=attachment_id, messageId=message_id
        ).execute()

        file_data = base64.urlsafe_b64decode(resp["data"])

        output_path = os.path.join(output_dir, filename)
        with open(output_path, "wb") as f:
            f.write(file_data)

        logger.info(
            f"Saved {content_type} attachment to {output_path} (size: {len(file_data)} bytes)"
        )
        return output_path

    def get_messages_in_thread(self, thread_id: str):
        thread = (
            self.client.users()
            .threads()
            .get(userId="me", id=thread_id, format="full")
            .execute()
        )

        return thread["messages"]

    def reply_to_thread(
        self,
        thread_id: str,
        body_text: str,
        to: Optional[Union[str, List[str]]] = None,
        to_address: Optional[str] = None,
        cc: Optional[Union[str, List[str]]] = None,
        bcc: Optional[Union[str, List[str]]] = None,
        subject: Optional[str] = None,
        in_reply_to_msgid: Optional[str] = None,
        create_draft: bool = True,
    ) -> dict:
        # TODO chatgpt output. Re-write and clean
        """Compose a reply in the given `thread_id`.

        - If `in_reply_to_msgid` or `to_address` or `subject` are not provided,
          the method will fetch the thread and use the most recent message's
          headers to fill them in.
        - By default a draft is created. Set `create_draft=False` to send
          the reply immediately (requires `https://www.googleapis.com/auth/gmail.send`).

        IMPORTANT: The OAuth token used must include appropriate scopes:
          - To create drafts/send messages: `https://www.googleapis.com/auth/gmail.compose` or `gmail.send`
          - To modify labels: `https://www.googleapis.com/auth/gmail.modify`

        Returns the API response from `drafts().create()` or `messages().send()`.
        """
        # If necessary, fetch thread to fill missing headers
        provided_to = to if to is not None else to_address
        if not (in_reply_to_msgid and provided_to and subject):
            try:
                thread = (
                    self.client.users()
                    .threads()
                    .get(userId="me", id=thread_id, format="full")
                    .execute()
                )
            except HttpError as http_error:
                logger.error(
                    f"Failed to fetch thread {thread_id} to prepare reply: {http_error}"
                )
                raise

            messages = thread.get("messages", [])
            if messages:
                last = messages[-1]
                headers = {
                    h.get("name", "").lower(): h.get("value")
                    for h in last.get("payload", {}).get("headers", [])
                }
                if not in_reply_to_msgid:
                    in_reply_to_msgid = headers.get("message-id")
                if not provided_to:
                    # reply-to or from header
                    provided_to = headers.get("reply-to") or headers.get(
                        "from"
                    )
                if not subject:
                    subj = headers.get("subject") or ""
                    if not subj.lower().startswith("re:"):
                        subject = f"Re: {subj}"
                    else:
                        subject = subj
            else:
                raise ValueError(
                    "Thread has no messages to reply to and required headers were not provided"
                )

        # normalize recipients: prefer `to` argument, fall back to `to_address` or discovered `provided_to`
        def _join_recipients(
            r: Optional[Union[str, List[str]]]
        ) -> Optional[str]:
            if r is None:
                return None
            if isinstance(r, list):
                return ", ".join(r)
            return r

        to_header = (
            _join_recipients(to)
            if to is not None
            else _join_recipients(to_address)
            if to_address is not None
            else provided_to
        )
        cc_header = _join_recipients(cc)
        bcc_header = _join_recipients(bcc)

        if not to_header:
            raise ValueError(
                "`to`/`to_address` must be provided either directly or via thread headers"
            )

        # build MIME message
        msg = MIMEText(body_text)
        msg["To"] = to_header
        if cc_header:
            msg["Cc"] = cc_header
        if bcc_header:
            msg["Bcc"] = bcc_header
        if subject:
            msg["Subject"] = subject
        if in_reply_to_msgid:
            msg["In-Reply-To"] = in_reply_to_msgid
            msg["References"] = in_reply_to_msgid

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        payload = {"message": {"raw": raw, "threadId": thread_id}}

        if create_draft:
            resp = (
                self.client.users()
                .drafts()
                .create(userId="me", body=payload)
                .execute()
            )
        else:
            resp = (
                self.client.users()
                .messages()
                .send(userId="me", body=payload)
                .execute()
            )

        return resp


def main():
    client = GmailClient()
    unread = client.list_messages()
    for message in unread["messages"]:
        thread_id = message["threadId"]
        client.get_message_details(thread_id)
        break


if __name__ == "__main__":
    main()
