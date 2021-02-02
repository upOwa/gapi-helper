import click
from simpletasks import Cli, Task

from gapi_helper.drive import DriveService, Folder


@click.group()
def drive():
    pass


@Cli(
    drive,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("user"),
    ],
)
class ListOwnedFilesTask(Task):
    def do(self) -> None:
        DriveService.configure(self.options.get("credentials"))
        user = self.options.get("user")
        delegatedService = DriveService(user).getService()

        page_token = None
        while True:
            response = (
                delegatedService.files()
                .list(
                    q="'{}' in owners".format(user),
                    spaces="drive",
                    fields="nextPageToken, files(id, mimeType, name)",
                    pageToken=page_token,
                )
                .execute()
            )

            for file in response.get("files", []):
                fileId = file.get("id")
                fileName = file.get("name")
                fileIsFolder = file.get("mimeType") == "application/vnd.google-apps.folder"
                print(f"Found {fileName} at {fileId} (is folder={fileIsFolder})")

            page_token = response.get("nextPageToken", None)

            if page_token is None:
                break


@Cli(
    drive,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("user"),
        click.argument("folderId"),
    ],
)
class ListFilesTask(Task):
    def do(self) -> None:
        DriveService.configure(self.options.get("credentials"))
        user = self.options.get("user")
        folderId = self.options.get("folderId")
        delegatedService = DriveService(user)

        folder = Folder("myfolder", folderId, client=delegatedService)
        lst = folder.list()
        if lst:
            for file in lst:
                fileId = file.get("id")
                fileName = file.get("name")
                fileIsFolder = file.get("mimeType") == "application/vnd.google-apps.folder"
                print(f"Found {fileName} at {fileId} (is folder={fileIsFolder})")
        else:
            print("Could not find folder")


if __name__ == "__main__":
    drive()
