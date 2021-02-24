# Google Drive API

See [/sample/drive/files.py](../../sample/drive/files.py) and [/sample/drive/sharewith.py](../../sample/drive/sharewith.py) for examples.

## Using the service directly

```python
from gapi_helper.drive import DriveService

DriveService.configure("credentials.json")
service = DriveService("myuser@mydomain.com").getService()

response = (
    service.files()
    .list(
        q="'{}' in owners".format("myuser@mydomain.com"),
        spaces="drive",
        fields="nextPageToken, files(id, mimeType, name)",
        pageToken=page_token,
    )
    .execute()
)

for file in response.get("files", []):
    fileId = file.get("id")
    fileIsFolder = file.get("mimeType") == "application/vnd.google-apps.folder"
    ...
```

## Using a Folder object

```python
from gapi_helper.drive import DriveService, Folder

DriveService.configure("credentials.json")
client = DriveService("myuser@mydomain.com")

folder = Folder("myfolder", "folderId", client=client)
lst = folder.list()
if lst:
    for file in lst:
        fileId = file.get("id")
        fileName = file.get("name")
        ...
else:
    print("Could not find folder")
```
