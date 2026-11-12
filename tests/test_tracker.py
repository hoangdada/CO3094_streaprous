
import unittest
from app import tracker


class TestTracker(unittest.TestCase):

    def setUp(self):
        tracker.peers.clear()
        tracker.contacts.clear()

    def test_login(self):
        result = tracker.login("alice")

        self.assertIn("alice", tracker.peers)
        self.assertFalse(result["online"])

    def test_duplicate_login(self):
        tracker.login("alice")
        tracker.login("alice")

        self.assertEqual(len(tracker.peers), 1)

    def test_submit_info(self):
        tracker.login("alice")

        result = tracker.submit_info("alice", "127.0.0.1", 9001)

        self.assertTrue(result)
        self.assertEqual(tracker.peers["alice"]["port"], 9001)
        self.assertTrue(tracker.peers["alice"]["online"])

    def test_submit_info_unknown_user(self):
        result = tracker.submit_info("unknown", "127.0.0.1", 9001)

        self.assertFalse(result)

    def test_get_list(self):
        tracker.login("alice")
        tracker.login("bob")
        tracker.submit_info("alice", "127.0.0.1", 9001)

        result = tracker.get_list()

        self.assertIn("alice", result)
        self.assertNotIn("bob", result)

    def test_find_peer(self):
        tracker.login("bob")
        tracker.submit_info("bob", "127.0.0.1", 9002)

        result = tracker.find_peer("bob")

        self.assertEqual(result["ip"], "127.0.0.1")
        self.assertEqual(result["port"], 9002)

    def test_find_unknown_peer(self):
        self.assertIsNone(tracker.find_peer("unknown"))

    def test_add_contact(self):
        tracker.login("alice")
        tracker.login("bob")

        result = tracker.add_contact("alice", "bob")

        self.assertTrue(result)
        self.assertIn("bob", tracker.contacts["alice"])

    def test_add_duplicate_contact(self):
        tracker.login("alice")
        tracker.login("bob")

        tracker.add_contact("alice", "bob")
        tracker.add_contact("alice", "bob")

        self.assertEqual(tracker.contacts["alice"], ["bob"])

    def test_add_unknown_contact(self):
        tracker.login("alice")

        result = tracker.add_contact("alice", "unknown")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
