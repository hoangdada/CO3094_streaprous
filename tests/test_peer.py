
import unittest

from app import peer


class TestPeer(unittest.TestCase):

    def setUp(self):
        peer.connections.clear()
        peer.INBOX_MESSAGES.clear()
        peer.MESSAGE_HISTORY.clear()

    def test_connect_peer(self):
        peer.connect_peer("bob", "127.0.0.1", 8002)

        peers = peer.get_connected_peers()

        self.assertEqual(len(peers), 1)
        self.assertEqual(peers[0]["username"], "bob")

    def test_duplicate_peer(self):
        peer.connect_peer("bob", "127.0.0.1", 8002)
        peer.connect_peer("bob", "127.0.0.1", 8002)

        self.assertEqual(len(peer.get_connected_peers()), 1)

    def test_disconnect_peer(self):
        peer.connect_peer("bob", "127.0.0.1", 8002)

        result = peer.disconnect_peer("127.0.0.1", 8002)

        self.assertTrue(result)
        self.assertEqual(len(peer.get_connected_peers()), 0)

    def test_receive_message(self):
        peer.push_incoming_p2p_message(
            "bob", "127.0.0.1", 8002,
            "alice", "unicast", "Hello",
            "msg-001", 123456
        )

        messages = peer.drain_inbox_messages()

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["text"], "Hello")

    def test_duplicate_message(self):
        for i in range(2):
            peer.push_incoming_p2p_message(
                "bob", "127.0.0.1", 8002,
                "alice", "unicast", "Hello",
                "msg-001", 123456
            )

        self.assertEqual(len(peer.drain_inbox_messages()), 1)

    def test_drain_inbox(self):
        peer.push_incoming_p2p_message(
            "bob", "127.0.0.1", 8002,
            "alice", "unicast", "Hello"
        )

        self.assertEqual(len(peer.drain_inbox_messages()), 1)
        self.assertEqual(len(peer.drain_inbox_messages()), 0)

    def test_save_sent_message(self):
        message = {"id": "msg-002", "text": "Hi"}

        peer.save_sent_message(message)

        self.assertEqual(len(peer.get_message_history()), 1)


if __name__ == "__main__":
    unittest.main()
