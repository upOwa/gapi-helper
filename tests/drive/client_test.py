from gapi_helper.drive import DriveService


def test_init(request_mock) -> None:
    assert DriveService.getDefaultService().getService() is not None
