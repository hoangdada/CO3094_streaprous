
import unittest
from unittest.mock import patch

from app import peer, peerapp


class TestPeerApp(unittest.TestCase):

    def setUp(self):
        peer.connections.clear()
        peer.INBOX_MESSAGES.clear()
        peer.MESSAGE_HISTORY.clear()
        peerapp.current_user = None

    @patch("app.peerapp.tracker_client.login")
    def test_login(self, mock_login):
        mock_login.return_value = {
            "status_code": 200,
            "data": {"status": "ok", "username": "alice"},
            "set_cookie": "session=abc"
        }

        result = peerapp.login({}, '{"username":"alice","password":"123"}')

        self.assertEqual(peerapp.current_user, "alice")
        self.assertEqual(result["status_code"], 200)

    def test_send_without_login(self):
        result = peerapp.send_peer({}, '{"target_ip":"127.0.0.1","target_port":8002,"message":"Hi"}')

        self.assertEqual(result["status_code"], 401)

    def test_send_without_connection(self):
        peerapp.current_user = "alice"

        result = peerapp.send_peer({}, '{"target_ip":"127.0.0.1","target_port":8002,"message":"Hi"}')

        self.assertEqual(result["data"]["status"], "error")

    @patch("app.peerapp.p2p_client.send_message")
    def test_send_success(self, mock_send):
        peerapp.current_user = "alice"
        peer.connect_peer("bob", "127.0.0.1", 8002)

        mock_send.return_value = {
            "status": "sent",
            "id": "msg-001",
            "timestamp": 123456,
            "message": {"id": "msg-001", "text": "Hi"}
        }

        result = peerapp.send_peer(
            {}, '{"target_ip":"127.0.0.1","target_port":8002,"message":"Hi"}'
        )

        self.assertEqual(result["status"], "sent")
        self.assertEqual(len(peer.get_message_history()), 1)

    def test_receive_message(self):
        result = peerapp.receive_p2p_message(
            {},
            '{"id":"msg-001","from_user":"bob","scope":"unicast","text":"Hello","timestamp":123456}'
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(peer.drain_inbox_messages()), 1)

    @patch("app.peerapp.tracker_client.get_list")
    def test_get_list(self, mock_get_list):
        mock_get_list.return_value = {
            "status_code": 200,
            "data": {
                "peers": [
                    {"username": "bob", "ip": "127.0.0.1", "port": 8002}
                ]
            },
            "set_cookie": None
        }

        peer.connect_peer("bob", "127.0.0.1", 8002)

        result = peerapp.get_list({}, None)

        self.assertEqual(len(result["peers"]), 1)
        self.assertTrue(result["peers"][0]["connected"])


if __name__ == "__main__":
    unittest.main()
