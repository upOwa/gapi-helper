import logging
import os
import queue
import threading
import time

import pytest

from gapi_helper.sheets import Sheet, SheetsService, Spreadsheet


@pytest.fixture(scope="function")
def service():
    credentials_old = SheetsService.credentials
    service_old = SheetsService.service
    headers_old = SheetsService.headers
    _sa_keyfile_old = SheetsService._sa_keyfile
    _spreadsheet_test_old = SheetsService._spreadsheet_test
    _backupLocation_old = SheetsService._backupLocation
    _cacheLocation_old = SheetsService._cacheLocation
    _logger_old = SheetsService._logger
    _force_test_spreadsheet_old = SheetsService._force_test_spreadsheet
    _retry_delay_old = SheetsService._retry_delay

    SheetsService.credentials = None
    SheetsService.service = None
    SheetsService.headers = None
    SheetsService._sa_keyfile = None
    SheetsService._spreadsheet_test = None
    SheetsService._backupLocation = None
    SheetsService._cacheLocation = None
    SheetsService._logger = logging.getLogger("gapi_helper")
    SheetsService._force_test_spreadsheet = False
    SheetsService._retry_delay = 5

    with pytest.raises(RuntimeError) as e:
        SheetsService.getService()
    assert str(e.value) == "Service is not configured"
    yield SheetsService

    SheetsService.credentials = credentials_old
    SheetsService.service = service_old
    SheetsService.headers = headers_old
    SheetsService._sa_keyfile = _sa_keyfile_old
    SheetsService._spreadsheet_test = _spreadsheet_test_old
    SheetsService._backupLocation = _backupLocation_old
    SheetsService._cacheLocation = _cacheLocation_old
    SheetsService._logger = _logger_old
    SheetsService._force_test_spreadsheet = _force_test_spreadsheet_old
    SheetsService._retry_delay = _retry_delay_old


_testfolder = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
_testspreadsheet = Spreadsheet("1E0iwdbALBLUBRbu1dfiOZ5xHkEwwB2xw223RM61c4uQ")
_testsheet = Sheet(_testspreadsheet, "CONTRATS SIGNES")
_serviceAccountCredentialsPath = "ucockpit-205209-1217fdd7529a.json"


@pytest.mark.slow
@pytest.mark.requireinternet
@pytest.mark.requireserviceaccount
def test_init(service) -> None:
    SheetsService.configure(
        os.path.join(_testfolder, _serviceAccountCredentialsPath),
        _testsheet,
        os.path.join(_testfolder, "data"),
        os.path.join(_testfolder, "cache"),
        force_test_spreadsheet=True,
        retry_delay=30.0,
    )

    assert SheetsService.getService() is not None


class InitThread(threading.Thread):
    def __init__(self, q: queue.Queue) -> None:
        threading.Thread.__init__(self)
        self.q = q

    def run(self) -> None:
        assert SheetsService.getService() is not None
        self.q.put(SheetsService.getHeaders()["Authorization"])


@pytest.mark.slow
@pytest.mark.requireinternet
@pytest.mark.requireserviceaccount
def test_multithread(service) -> None:
    SheetsService.configure(
        os.path.join(_testfolder, _serviceAccountCredentialsPath),
        _testsheet,
        os.path.join(_testfolder, "data"),
        os.path.join(_testfolder, "cache"),
        force_test_spreadsheet=True,
    )

    threads = []
    q: queue.Queue[str] = queue.Queue()
    for i in range(5):
        thread = InitThread(q)
        threads.append(thread)
        thread.start()
        time.sleep(1)

    for thread in threads:
        thread.join()

    headers = None
    while not q.empty():
        result = q.get()
        if headers is None:
            headers = result
        assert result == headers
