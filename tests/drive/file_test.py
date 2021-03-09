from gapi_helper.drive import File, Folder

from .conftest import read_datafile


def test_init(request_mock) -> None:
    f = File("myfile", "0123456")
    assert f.client is not None


def test_share(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("sharefile.json", "rb")),
    )
    f = File("myfile", "0123456")
    assert f.share("user@domain.com", "writer", False) == f


def test_transfer_ownership(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("transferownership.json", "rb")),
    )

    f = File("myfile", "0123456")
    assert f.transfer_ownership("user@domain.com") == f


def test_delete(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "204"}, "{}"),
    )
    File("myfile", "0123456").delete()


def test_copyTo(request_mock) -> None:
    request_mock._iterable.append(
        ({"status": "200"}, read_datafile("copyfile.json", "rb")),
    )

    f = File("myfile", "0123456")
    folder = Folder("myfolder", "abcdef")
    new_file = f.copyTo(folder, "New name")
    assert new_file.file_id == "MyNewId__"
    assert new_file.file_name == "Copy"
