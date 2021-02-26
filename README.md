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

```bash
poetry install --no-root
```

To run tests (including linting and code formatting checks), please run:

```bash
poetry run pytest --mypy --flake8 && poetry run black --check .
```

### Tips

How to generate requests mocks:

1. Put breakpoints in _.venv/lib/python3.6/site-packages/googleapiclient/http.py:211_ (end of `_retry_request` method)
2. Create a script that will do the actions
3. Ensure the debugger is configured to debug external code (`"justMyCode": false` in VSCode)
4. Debug that script
5. Save all results to `_retry_request` (status and *anonymized* content) to files in _data_ folder (if not already existing)
