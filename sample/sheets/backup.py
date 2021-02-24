import tempfile

import click
from simpletasks import Cli, CliParams, Task

from gapi_helper.drive import DriveService
from gapi_helper.drive.folder import Folder
from gapi_helper.sheets import SheetsService, Spreadsheet


@click.group()
def sheets():
    pass


@Cli(
    sheets,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("fileIdSource"),
        click.argument("fileIdDestination"),
        CliParams.dryrun(),
    ],
)
class DumpSheetTask(Task):
    def do(self) -> None:
        with tempfile.TemporaryDirectory() as _cacheDir:
            DriveService.configure(self.options.get("credentials"))
            SheetsService.configure(
                self.options.get("credentials"),
                Spreadsheet("1gzHZjlE3M1tYmHghiSeyBaasGH99mYdvnTZZZNzBu4E").addSheet("MySheet1"),
                _cacheDir,
                _cacheDir,
            )

            source = Spreadsheet(self.options.get("fileidsource"))
            destination = Spreadsheet(self.options.get("fileiddestination"))

            source.dumpTo(destination, dryrun=self.dryrun)


@Cli(
    sheets,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("user"),
        click.argument("fileIdSource"),
        click.argument("folderIdDestination"),
        CliParams.dryrun(),
    ],
)
class DuplicateSheetTask(Task):
    def do(self) -> None:
        with tempfile.TemporaryDirectory() as _cacheDir:
            DriveService.configure(self.options.get("credentials"))
            SheetsService.configure(
                self.options.get("credentials"),
                Spreadsheet("1gzHZjlE3M1tYmHghiSeyBaasGH99mYdvnTZZZNzBu4E").addSheet("MySheet1"),
                _cacheDir,
                _cacheDir,
            )

            source = Spreadsheet(self.options.get("fileidsource"))
            folder_id = self.options.get("folderiddestination")
            folder = Folder(folder_id, folder_id, client=DriveService(self.options.get("user")))

            source.dumpIn(folder, "test", dryrun=self.dryrun)


@Cli(
    sheets,
    params=[
        click.argument(
            "credentials",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True),
        ),
        click.argument("user"),
        click.argument("fileIdSource"),
        click.argument("folderIdDestination"),
        CliParams.dryrun(),
    ],
)
class CopySheetTask(Task):
    def do(self) -> None:
        with tempfile.TemporaryDirectory() as _cacheDir:
            DriveService.configure(self.options.get("credentials"))
            SheetsService.configure(
                self.options.get("credentials"),
                Spreadsheet("1gzHZjlE3M1tYmHghiSeyBaasGH99mYdvnTZZZNzBu4E").addSheet("MySheet1"),
                _cacheDir,
                _cacheDir,
            )

            source = Spreadsheet(self.options.get("fileidsource"))
            folder_id = self.options.get("folderiddestination")
            folder = Folder(folder_id, folder_id, client=DriveService(self.options.get("user")))

            source.copyIn(folder, "test", dryrun=self.dryrun)


if __name__ == "__main__":
    sheets()
