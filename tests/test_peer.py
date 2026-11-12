
import unittest
import socket
import threading
from app import peer


class TestPeer(unittest.TestCase):

    def setUp(self):
        peer.connections.clear()

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(5)
        self.server.settimeout(3)

        self.port = self.server.getsockname()[1]
        self.received = []
        self.threads = []

    def tearDown(self):
        for sock in list(peer.connections.values()):
            sock.close()

        peer.connections.clear()
        self.server.close()

        for thread in self.threads:
            thread.join(timeout=3)

    def start_receiver(self, count=1):
        def receive():
            try:
                for _ in range(count):
                    conn, addr = self.server.accept()
                    with conn:
                        conn.settimeout(3)
                        data = conn.recv(4096)
                        self.received.append(data.decode("utf-8"))
            except (OSError, TimeoutError):
                pass

        thread = threading.Thread(target=receive)
        thread.start()
        self.threads.append(thread)

    def test_connect_peer(self):
        result = peer.connect_peer("bob", "127.0.0.1", self.port)

        self.assertTrue(result)
        self.assertIn("bob", peer.connections)

    def test_send_message(self):
        self.start_receiver()

        peer.connect_peer("bob", "127.0.0.1", self.port)
        result = peer.send_peer("bob", "Hello Bob")

        self.assertTrue(result)

        for thread in self.threads:
            thread.join(timeout=3)

        self.assertEqual(self.received, ["Hello Bob"])

    def test_send_without_connection(self):
        result = peer.send_peer("unknown", "Hello")

        self.assertFalse(result)

    def test_broadcast(self):
        self.start_receiver(count=2)

        peer.connect_peer("bob", "127.0.0.1", self.port)
        peer.connect_peer("charlie", "127.0.0.1", self.port)

        results = peer.broadcast_peer("Hello everyone")

        self.assertTrue(results["bob"])
        self.assertTrue(results["charlie"])

        for thread in self.threads:
            thread.join(timeout=3)

        self.assertEqual(
            self.received.count("Hello everyone"), 2
        )

    def test_connection_failed(self):
        # Use a port that was opened and then closed
        temp = socket.socket()
        temp.bind(("127.0.0.1", 0))
        unused_port = temp.getsockname()[1]
        temp.close()

        result = peer.connect_peer(
            "unknown", "127.0.0.1", unused_port
        )

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
