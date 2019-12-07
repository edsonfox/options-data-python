"""Tests for OptionsDataDownloader module."""

# Standard libraries
import unittest
from unittest import mock
from requests import Session

# External dependencies

# Application-specific imports
from options_data_downloader import OptionsDataDownloader, TOS_OPTION_CHAIN_API_URL


def mocked_session_get(*args, **kwargs):  # pylint: disable=W0613
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def status(self):
            return self.status_code

    if args[0].startswith(TOS_OPTION_CHAIN_API_URL):
        return MockResponse({"status": "PASSED"}, 200)
    return MockResponse(None, 404)


class TestOptionsDataDownloader(unittest.TestCase):
    @mock.patch.object(Session, "get", side_effect=mocked_session_get)
    def test_fetch(self, mock_get):
        downloader = OptionsDataDownloader()
        json_data = downloader.get_option_chain_data("TSLA")
        self.assertEqual(json_data, {"status": "PASSED"})
        self.assertEqual(len(mock_get.call_args_list), 1)


if __name__ == "__main__":
    unittest.main()
