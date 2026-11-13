
import unittest
import json
from unittest.mock import patch

from app import p2p_client


class TestP2PClient(unittest.TestCase):

    def test_build_request(self):
        request = p2p_client.build_request(
            "/p2p-message/",
            {"text": "Hello"}
        )

        header, body = request.split(b"\r\n\r\n", 1)

        self.assertIn(b"POST /p2p-message/ HTTP/1.1", header)
        self.assertIn(
            f"Content-Length: {len(body)}".encode(),
            header
        )
        self.assertEqual(json.loads(body), {"text": "Hello"})

    def test_parse_response_success(self):
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"\r\n"
            b'{"status":"ok"}'
        )

        result = p2p_client.parse_response(response)

        self.assertEqual(result["status"], "ok")

    def test_parse_response_error(self):
        response = (
            b"HTTP/1.1 400 Bad Request\r\n"
            b"\r\n"
            b'{"status":"error"}'
        )

        with self.assertRaises(ConnectionError):
            p2p_client.parse_response(response)

    @patch("app.p2p_client.send_request")
    def test_send_message(self, mock_send):
        mock_send.return_value = {"status": "ok"}

        result = p2p_client.send_message(
            "127.0.0.1", 8002,
            "alice", "bob", "Hello"
        )

        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["message"]["from_user"], "alice")
        self.assertEqual(result["message"]["to_user"], "bob")

    @patch("app.p2p_client.send_request")
    def test_broadcast(self, mock_send):
        mock_send.side_effect = [
            {"status": "ok"},
            ConnectionError("Peer offline"),
            {"status": "ok"}
        ]

        peers = [
            {"username": "bob", "ip": "127.0.0.1", "port": 8002},
            {"username": "charlie", "ip": "127.0.0.1", "port": 8003},
            {"username": "david", "ip": "127.0.0.1", "port": 8004}
        ]

        result = p2p_client.broadcast_message(
            peers, "alice", "Hello everyone"
        )

        self.assertEqual(result["count"], 2)


if __name__ == "__main__":
    unittest.main()
