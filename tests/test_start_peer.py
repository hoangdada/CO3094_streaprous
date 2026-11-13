
import unittest
from unittest.mock import patch

import start_peer


class TestStartPeer(unittest.TestCase):

    @patch("start_peer.create_peerapp")
    @patch("start_peer.p2p_client.set_local_address")
    @patch("start_peer.tracker_client.set_tracker")
    @patch("sys.argv", [
        "start_peer.py",
        "--ip", "127.0.0.1",
        "--port", "8001",
        "--tracker-ip", "127.0.0.1",
        "--tracker-port", "8000"
    ])
    def test_startup(self, mock_tracker, mock_local, mock_create):
        start_peer.main()

        mock_tracker.assert_called_once_with("127.0.0.1", 8000)
        mock_local.assert_called_once_with("127.0.0.1", 8001)
        mock_create.assert_called_once_with("127.0.0.1", 8001)


if __name__ == "__main__":
    unittest.main()
