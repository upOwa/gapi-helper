import queue
import threading

import pytest

from gapi_helper.sheets import SheetsService


def test_init(request_mock) -> None:
    assert SheetsService.getService() is not None


class InitThread(threading.Thread):
    def __init__(self, q: queue.Queue) -> None:
        threading.Thread.__init__(self)
        self.q = q

    def run(self) -> None:
        assert SheetsService.getService() is not None
        self.q.put(SheetsService.getHeaders()["Authorization"])


@pytest.mark.slow
def test_multithread(request_mock) -> None:
    threads = []
    q: "queue.Queue[str]" = queue.Queue()
    for i in range(5):
        thread = InitThread(q)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    headers = None
    while not q.empty():
        result = q.get()
        if headers is None:
            headers = result
        assert result == headers
