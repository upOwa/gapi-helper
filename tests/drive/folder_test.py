import os
import tempfile

from gapi_helper.drive import Folder

from .conftest import read_datafile


def test_init(request_mock) -> None:
    f = Folder("myfile", "0123456")
    assert f.client is not None


def test_findFile(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("findfile.json", "rb")),
    )
    f = Folder("myfolder", "0123456").findFile("My File")
    assert f is not None
    assert f["name"] == "My File"
    assert f["id"] == "ABCDEF"


def test_list(request_mock) -> None:
    request_mock._iterable.append(({"status": "200"}, read_datafile("listfolder1.json", "rb")))
    request_mock._iterable.append(({"status": "200"}, read_datafile("listfolder2.json", "rb")))
    request_mock._iterable.append(({"status": "200"}, read_datafile("listfolder3.json", "rb")))
    f = Folder("myfolder", "0123456").list()
    assert f is not None
    assert len(f) == 14
    assert f[0]["name"] == "File 1"
    assert f[0]["id"] == "IzKnjveb1mZ6TDkmO2BZ"

    assert f[13]["name"] == "File 14"
    assert f[13]["id"] == "c39VQkQtoMhZMYnEDqqn"


def test_downloadFile(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("findfile-text.json", "rb")),
    )
    request_mock._iterable.append(
        ({"status": "206"}, "test"),
    )
    f = Folder("myfolder", "0123456")
    with tempfile.NamedTemporaryFile("w") as tmp:
        path = f.downloadFile("test.txt", tmp.name)
        assert path == tmp.name
        with open(tmp.name, "r", encoding="utf-8") as fp:
            assert fp.read() == "test"


def test_hasFile(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("findfile-text.json", "rb")),
    )
    request_mock._iterable.append(
        ({"status": "206"}, "test"),
    )
    f = Folder("myfolder", "0123456")
    with tempfile.TemporaryDirectory() as tmp:
        downloadpath = os.path.join(tmp, "test.txt")
        path = f.hasFile("test.txt", downloadpath)
        assert path == downloadpath
        with open(downloadpath, "r", encoding="utf-8") as fp:
            assert fp.read() == "test"
        assert f.hasFile("test.txt", downloadpath) == downloadpath


def test_uploadFile(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("uploadfile.json", "rb")),
    )
    f = Folder("myfolder", "0123456")
    with tempfile.NamedTemporaryFile("w") as tmp:
        with open(tmp.name, "w", encoding="utf-8") as fp:
            fp.write("test")

        id = f.uploadFile(tmp.name, "text/plain", update=False)
        assert id == "TESTLNyXuunGewRG7dSV6Qia50wP7-xiL"


def test_uploadFile_update(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("findfile-text.json", "rb")),
    )
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("uploadfile.json", "rb")),
    )
    f = Folder("myfolder", "0123456")
    with tempfile.NamedTemporaryFile("w") as tmp:
        with open(tmp.name, "w", encoding="utf-8") as fp:
            fp.write("test")

        id = f.uploadFile(tmp.name, "text/plain", update=True)
        assert id == "TESTLNyXuunGewRG7dSV6Qia50wP7-xiL"


def test_insertFile(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("uploadfile.json", "rb")),
    )
    f = Folder("myfolder", "0123456")
    newfile = f.insertFile("Test")
    assert newfile is not None
    assert newfile.file_id == "TESTLNyXuunGewRG7dSV6Qia50wP7-xiL"
    assert newfile.file_name == "Test"
    assert newfile.client == f.client


def test_createFolder(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("uploadfile.json", "rb")),
    )
    f = Folder("myfolder", "0123456")
    newfolder = f.createFolder("Test")
    assert newfolder is not None
    assert newfolder.file_id == "TESTLNyXuunGewRG7dSV6Qia50wP7-xiL"
    assert newfolder.file_name == "Test"
    assert newfolder.client == f.client
