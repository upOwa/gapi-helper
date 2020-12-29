import base64
import email.message
import email.mime.base
import email.mime.multipart
import email.mime.text
import logging
import mimetypes
import os
import threading
from typing import Any, Iterable, Optional

from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


class MailService:
    _sa_keyfile: Optional[str] = None
    _logger = logging.getLogger("gapi_helper")
    _lock = threading.Lock()
    _to_test: Optional[str] = None
    _force_test_email = False

    @staticmethod
    def configure(
        sa_keyfile: str, to_test: str, force_test_email: bool = False, logger_namespace: str = None
    ) -> None:
        MailService._sa_keyfile = sa_keyfile
        if logger_namespace:
            MailService._logger = logging.getLogger(logger_namespace)
        MailService._to_test = to_test
        MailService._force_test_email = force_test_email

    def __init__(self, sender: str, logger_namespace: str = None) -> None:
        self._sender = sender
        self.credentials: Any = None
        self.service: Any = None
        if logger_namespace:
            self._logger = logging.getLogger(logger_namespace)
        else:
            self._logger = MailService._logger

    def getService(self) -> Any:
        with MailService._lock:
            if self.service is None:
                if MailService._sa_keyfile is None or self._sender is None:
                    raise RuntimeError("Service is not configured")

                credentials = ServiceAccountCredentials.from_json_keyfile_name(
                    MailService._sa_keyfile,
                    (
                        "https://mail.google.com/",
                        "https://www.googleapis.com/auth/gmail.compose",
                        "https://www.googleapis.com/auth/gmail.modify",
                        "https://www.googleapis.com/auth/gmail.send",
                    ),
                )
                self.credentials = credentials.create_delegated(self._sender)
                self.service = build("gmail", "v1", credentials=self.credentials)

            return self.service

    def reset(self) -> None:
        with MailService._lock:
            self.service = None

    def build_message(
        self,
        to: str,
        subject: str,
        content_raw: str,
        replyto: Optional[str] = None,
        cc: Optional[Iterable[str]] = None,
        filepath: Optional[str] = None,
        filetype: Optional[str] = None,
    ) -> email.message.EmailMessage:
        message = email.message.EmailMessage()
        message["To"] = to if not MailService._force_test_email else MailService._to_test
        message["From"] = self._sender
        if replyto:
            message["Reply-To"] = replyto
        if cc and not MailService._force_test_email:
            message["CC"] = ",".join(cc)
        message["Subject"] = subject

        message.set_content(content_raw)

        if filepath:
            content_type, encoding = mimetypes.guess_type(filepath)
            if content_type is None or encoding is not None:
                content_type = filetype if filetype is not None else "application/octet-stream"
            main_type, sub_type = content_type.split("/", 1)
            filename = os.path.basename(filepath)

            with open(filepath, "rb") as fp:
                data = fp.read()
                message.add_attachment(data, maintype=main_type, subtype=sub_type, filename=filename)
        return message

    def send_message(self, message: email.message.EmailMessage) -> str:
        payload = {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}
        res = self.getService().users().messages().send(userId=self._sender, body=payload).execute()
        if "id" not in res:
            raise RuntimeError("Could not send email: {}".format(message.items()))
        return res["id"]

    def trash_message(self, id: str) -> None:
        self.getService().users().messages().trash(userId=self._sender, id=id).execute()

    def send_and_trash_message(self, message: email.message.EmailMessage) -> str:
        msg_id = self.send_message(message)
        if msg_id:
            self.trash_message(msg_id)
        return msg_id
