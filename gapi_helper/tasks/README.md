# Extensions for simpletasks and simpletasks-data

## Import

You can use `ImportSheet` as a source for [simpletasks-data](https://github.com/upOwa/simpletasks-data) `ImportTask`:

```python
from typing import Optional, Sequence

from flask.cli import AppGroup
from gapi_helper.sheets import Sheet, Spreadsheet
from gapi_helper.tasks import ImportSheet
import geoalchemy2
from simpletasks import Cli, CliParams
from simpletasks_data import ImportSource, ImportTask, Mapping

from myapp import db

cli = AppGroup('mytasks')

class Region(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(256), nullable=False)
    population = db.Column(db.Integer)
    center = db.Column(
        geoalchemy2.Geography(geometry_type="POINT", srid=4326)
    )

mysheet = Spreadsheet('abcdef').addSheet('Regions')

def toGpsCoordinates(gps: Optional[str]) -> Optional[geoalchemy2.elements.WKBElement]:
    if gps:
        point = shapely.wkt.loads(gps)
        return geoalchemy2.shape.from_shape(point, srid=4326)
    return None

def compareGpsCoordinates(
    x: Optional[geoalchemy2.elements.WKBElement], y: Optional[geoalchemy2.elements.WKBElement]
) -> bool:
    if x == y:
        return True

    if x and y:
        # Precision loss
        shape1 = geoalchemy2.shape.to_shape(x)
        shape2 = geoalchemy2.shape.to_shape(y)
        return shape1.almost_equals(shape2)
    else:
        return False

@Cli(cli, params=[CliParams.progress(), CliParams.dryrun()])
class ImportRegionsTask(ImportTask):
    class _RegionsSource(ImportSheet):
        class _RegionsMapping(Mapping):
            def __init__(self) -> None:
                super().__init__()

                self.id = self.auto()
                self.name = self.auto()
                self.population = self.auto()
                self.center = self.auto(parser=toGpsCoordinates, comparator=compareGpsCoordinates)

        def __init__(self) -> None:
            super().__init__(mysheet, mapping=self._RegionsMapping())

    def createModel(self) -> Region:
        return Region()

    def get_sources(self) -> Iterable[ImportSource]:
        return [self._RegionsSource()]

    def __init__(self, *args, **kwargs):
        super().__init__(model=Region(), *args, **kwargs)

```

## Export

This library adds `TransferTask` to write data into one or several Google Sheets.

You can either directly inherit from `TransferTask` to write custom data, or use:
* `TransferCsvTask` to write CSV data
* `TransferSheetTask` to write from a source Google Sheet


The simplest is `TransferSheetTask`:

```python
class MyTask(TransferSheetTask):
    def __init__(self, *args, **kwargs) -> None:
        # Download content of Source on-the-fly
        super().__init__(Spreadsheet('abcdef').addSheet('Source'), use_cache=False, *args, **kwargs)

    def getDestinations(self) -> List[TransferDestination]:
        return [
            # Copy range Source!A1:N into Destination1!A1:N
            TransferDestination(Spreadsheet('bcdefa').addSheet('Destination1'), [("A1:N", "A1:N")]),
            
            # Copy ranges:
            # - Source!A2:A into Destination2!A1:A
            # - Source!N2:A into Destination2!B1:B
            TransferDestination(Spreadsheet('cdefab').addSheet('Destination2'), [("A2:A", "A1:A"), ("N2:N", "B1:B")]),
        ]
```

In this example, when running `MyTask`:
1. data from sheet `Source` (from spreadsheet `abcdef`) will be downloaded into memory
2. range A1:N will be copied to `Destination1` (spreadsheet `bcdefa`) at A1:N
3. range A2:A will be copied to `Destination2` (spreadsheet `cdefab`) at A1:A
4. range N2:N will be copied to `Destination2` (spreadsheet `cdefab`) at B1:B


The philosophy with `TransferTask` is the same, but you'll have to generate your own data via implementing `getData`.
