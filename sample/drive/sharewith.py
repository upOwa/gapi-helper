import click
from simpletasks import Cli, Task

from gapi_helper.drive import DriveService, File


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
        click.argument("fileId"),
        click.argument("user"),
        click.option(
            "--role",
            type=click.Choice(["reader", "commenter", "writer", "owner"]),
            default="writer",
            help="Role of the permission to create",
        ),
    ],
)
class ShareWithTask(Task):
    def do(self) -> None:
        DriveService.configure(self.options.get("credentials"))
        user = self.options.get("user")
        role = self.options.get("role")
        fileId = self.options.get("fileid")

        file = File(fileId, fileId)

        if role == "owner":
            file.transfer_ownership(user)
        else:
            file.share(user, role, False)


@Cli(
    drive,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("fileId"),
    ],
)
class DeleteTask(Task):
    def do(self) -> None:
        DriveService.configure(self.options.get("credentials"))
        fileId = self.options.get("fileid")

        file = File(fileId, fileId)
        file.delete()


if __name__ == "__main__":
    drive()
