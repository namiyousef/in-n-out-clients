import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid

# from email.mime.text import MIMEText


logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
logger = logging.getLogger(__name__)
SMTP_SETTINGS = {
    "gmail": {
        "server_address": "smtp.gmail.com",
        "port": {"ssl": 465, "tls": 587},
    },
    "outlook": {"server_address": "smtp.office365.com", "port": {"tls": 587}},
}


class EmailClient:
    def __init__(self, provider, email_params, connection_strategy="tls"):
        self.provider = provider
        self.connection_strategy = connection_strategy
        self.sender_email = email_params["sender_email"]
        self.recipient_email = email_params["recipient_email"]
        self.password = email_params["password"]
        self.in_reply_to = email_params.get("message_id")
        self.subject = email_params.get("subject")
        self.content = email_params.get("content")

        self.message_id = make_msgid()

        self._infer_smtp_settings

        self.build_message

    @property
    def _infer_smtp_settings(self):
        try:
            smtp_settings = SMTP_SETTINGS[self.provider]
        except KeyError as key_error:
            raise ValueError(
                (
                    f"Provider `{self.provider}` is not an acceptable type. "
                    f"Please choose a provider from the following "
                    f"{tuple(SMTP_SETTINGS.keys())}."
                )
            ) from key_error

        try:
            self.server_address = smtp_settings["server_address"]
            self.port = smtp_settings["port"][self.connection_strategy]
        except KeyError as key_error:
            raise ValueError(
                (
                    f"Provider `{self.provider}` does not support "
                    f"`{self.connection_strategy}` connection strategy"
                )
            ) from key_error

    @property
    def build_message(self):
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = " ".join(self.recipient_email)
        message["Subject"] = self.subject
        message["Message-ID"] = self.message_id
        message["In-Reply-To"] = self.in_reply_to
        message["References"] = self.in_reply_to
        self.message = message

    def add_attachment(self, content, filename):
        # Open PDF file in binary mode
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(content)

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

        # Add attachment to message and convert message to string
        self.message.attach(part)

    def _connect_tls(self):
        session = smtplib.SMTP(self.server_address, self.port)
        session.starttls()
        return session

    def _connect_ssl(self):
        session = smtplib.SMTP_SSL(self.server_address, self.port)
        return session

    def send_email(self):
        logger.info(
            (
                f"Starting `{self.connection_strategy}` connection to "
                f"`{self.provider} SMTP server...`"
            )
        )

        session = getattr(self, f"_connect_{self.connection_strategy}")()

        logger.info("Connection made. Logging in...")
        session.login(
            self.sender_email, self.password
        )  # login with mail_id and password
        text = self.message.as_string()
        session.sendmail(self.sender_email, self.recipient_email, text)
        logger.info("Email sent")
        session.quit()
