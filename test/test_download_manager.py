from datetime import datetime, timedelta

from xcube_clms.constants import TIME_TO_EXPIRE
from xcube_clms.download_manager import (
    get_dataset_download_info,
    get_authorization_header,
    has_expired,
)


def test_get_dataset_download_info():
    dataset_id = "dataset123"
    file_id = "file456"
    expected_result = {
        "Datasets": [
            {
                "DatasetID": dataset_id,
                "FileID": file_id,
            }
        ]
    }
    result = get_dataset_download_info(dataset_id, file_id)
    assert result == expected_result


def test_get_authorization_header():
    token = "test_token"
    expected = {"Authorization": "Bearer test_token"}
    assert get_authorization_header(token) == expected


def test_has_expired_not_expired():
    download_available_time = (datetime.now() - timedelta(hours=1)).isoformat()
    assert not has_expired(download_available_time)


def test_has_expired_expired():
    download_available_time = (
        datetime.now() - timedelta(hours=TIME_TO_EXPIRE + 1)
    ).isoformat()
    assert has_expired(download_available_time)
