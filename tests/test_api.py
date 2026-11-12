
import unittest
import json

from app import sampleapp, tracker, peer
from unittest.mock import patch


class TestAPI(unittest.TestCase):

    def setUp(self):
        tracker.peers.clear()
        tracker.contacts.clear()
        peer.connections.clear()

    def call_api(self, handler, data=None):
        body = json.dumps(data) if data is not None else ""
        response = handler({}, body)
        return json.loads(response)

    def test_login(self):
        result = self.call_api(
            sampleapp.login,
            {"username": "alice"}
        )

        self.assertEqual(result["status"], "success")
        self.assertIn("alice", tracker.peers)

    def test_login_missing_username(self):
        result = self.call_api(sampleapp.login, {})

        self.assertIn("error", result)

    def test_submit_info(self):
        tracker.login("alice")

        result = self.call_api(
            sampleapp.submit_info,
            {
                "username": "alice",
                "ip": "127.0.0.1",
                "port": 9001
            }
        )

        self.assertEqual(result["status"], "success")

    def test_add_list(self):
        tracker.login("alice")
        tracker.login("bob")

        result = self.call_api(
            sampleapp.add_list,
            {"username": "alice", "peer": "bob"}
        )

        self.assertEqual(result["status"], "success")

    def test_get_list(self):
        tracker.login("alice")
        tracker.submit_info("alice", "127.0.0.1", 9001)

        result = self.call_api(sampleapp.get_list)

        self.assertIn("alice", result["peers"])

    @patch("app.sampleapp.peer.connect_peer", return_value=True)
    def test_connect_peer(self, mock_connect):
        tracker.login("bob")
        tracker.submit_info("bob", "127.0.0.1", 9002)

        result = self.call_api(
            sampleapp.connect_peer,
            {"peer": "bob"}
        )

        self.assertEqual(result["status"], "connected")
        mock_connect.assert_called_once_with(
            "bob", "127.0.0.1", 9002
        )

    @patch("app.sampleapp.peer.send_peer", return_value=True)
    def test_send_peer(self, mock_send):
        result = self.call_api(
            sampleapp.send_peer,
            {"peer": "bob", "message": "Hello Bob"}
        )

        self.assertEqual(result["status"], "sent")
        mock_send.assert_called_once_with("bob", "Hello Bob")

    @patch("app.sampleapp.peer.broadcast_peer")
    def test_broadcast_peer(self, mock_broadcast):
        mock_broadcast.return_value = {
            "bob": True,
            "charlie": True
        }

        result = self.call_api(
            sampleapp.broadcast_peer,
            {"message": "Hello everyone"}
        )

        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["results"]["bob"])

    def test_invalid_json(self):
        response = sampleapp.login({}, "{invalid json}")
        result = json.loads(response)

        self.assertIn("error", result)


if __name__ == "__main__":
    unittest.main()
