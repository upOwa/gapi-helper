# gapi-helper

Helpers around Google APIs:
- [Google Drive API](gapi_helper/drive/README.md)
- [Google Mail API](gapi_helper/mail/README.md)
- [Google Sheets API](gapi_helper/sheets/README.md)

Also provides new classes for [simpletasks-data](https://github.com/upOwa/simpletasks-data):
- `DumpTask` to dump a [Flask-SQLAlchemy](https://flask-sqlalchemy.palletsprojects.com/) model into a Google Sheet
- `TransferTask` to write arbitrary data to a Google Sheet
  - `TransferCsvTask` to write CSV data to a Google Sheet
  - `TransferSheetTask` to write a Google Sheet to another Google Sheet
- `ImportSheet` to use a Google Sheet as source for `ImportTask`

## Contributing

To initialize the environment:
```
poetry install --no-root
```

To run tests (including linting and code formatting checks), please run:
```
poetry run pytest --mypy --flake8 && poetry run black --check .
```
