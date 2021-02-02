# Google Mail API

```python
from gapi_helper.mail import MailService

MailService.configure("credentials.json")
service = MailService("myuser@mydomain.com") # Sender

content = """Hello, world!
This is a sample email.
"""

message = service.build_message(
    to="recipient@mydomain.com,
    subject="My important message",
    content_raw=content,
    filepath="/foo/bar.pdf", # Attach a file
)
```
