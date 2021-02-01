# Google Sheets API

```python
from gapi_helper.sheets import SheetsService, Sheet, Spreadsheet

SheetsService.configure(
    "credentials.json",
    Spreadsheet("abcdef").addSheet("TEST"),
    "/foo/backups",
    "/foo/cache",
)

my_spreadsheet = Spreadsheet("fedcba").addSheet("My Sheet")
path = my_spreadsheet.download() # Download as CSV file

with my_spreadsheet.csvreader() as csvreader:
    for row in csvreader:
        # Iterate the Sheet as CSV
        ...

# Write data into the sheet
my_spreadsheet.bulkWrite(
    [
        ["Hello", "World"],
        ["Second", "line"],
        ["Third", "line"],
    ],
    0, # First row
    0, # First column
)
```
