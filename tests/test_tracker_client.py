
import unittest
from unittest.mock import patch, MagicMock

from app import tracker_client


class TestTrackerClient(unittest.TestCase):

    @patch("app.tracker_client.http.client.HTTPConnection")
    def test_login(self, mock_connection):
        response = MagicMock()
        response.status = 200
        response.read.return_value = b'{"status":"ok","username":"alice"}'
        response.getheader.return_value = "session=abc"

        conn = mock_connection.return_value
        conn.getresponse.return_value = response

        result = tracker_client.login("alice", "123")

        self.assertEqual(result["status_code"], 200)
        self.assertEqual(result["data"]["username"], "alice")
        self.assertEqual(result["set_cookie"], "session=abc")

        args = conn.request.call_args.args
        self.assertEqual(args[0], "POST")
        self.assertEqual(args[1], "/login/")

    @patch("app.tracker_client.http.client.HTTPConnection")
    def test_tracker_unavailable(self, mock_connection):
        conn = mock_connection.return_value
        conn.request.side_effect = OSError("Connection refused")

        result = tracker_client.get_list()

        self.assertEqual(result["status_code"], 502)
        self.assertEqual(result["data"]["status"], "error")


if __name__ == "__main__":
    unittest.main()
